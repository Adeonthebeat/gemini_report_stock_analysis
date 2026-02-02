import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import markdown
import pandas as pd
from google import genai
from datetime import datetime, timedelta
from jinja2 import Environment, FileSystemLoader
from prefect import task, get_run_logger
from sqlalchemy import text
from app.core.database import get_engine
from app.core.config import GOOGLE_API_KEY, BASE_DIR

# [NEW] 스캐너 모듈 가져오기
from app.services.scanner import scan_breakout_stocks


# ---------------------------------------------------------
# [Helper] 신호등 판별 함수
# ---------------------------------------------------------
def classify_status(row):
    """
    재무 데이터를 기반으로 신호등 이모지 반환
    """
    net_income = row.get('net_income') or 0
    rev_growth = row.get('rev_growth_yoy') or 0
    eps_growth = row.get('eps_growth_yoy') or 0

    if net_income > 0 and (rev_growth > 0 or eps_growth > 0):
        return "🟢 우량(성장)"
    elif net_income > 0:
        return "🟢 흑자"
    elif (rev_growth > 0) or (eps_growth > 0):
        return "🟡 적자(성장중)"
    else:
        return "🔴 위험"


@task(name="Generate-AI-Report")
def generate_ai_report():
    logger = get_run_logger()
    engine = get_engine()

    if not GOOGLE_API_KEY:
        logger.error("GOOGLE_API_KEY가 설정되지 않았습니다.")
        return

    client = genai.Client(api_key=GOOGLE_API_KEY)

    # =========================================================
    # [STEP 1] 섹터(Sector) 랭킹 데이터 추출 (Top-Down)
    # =========================================================
    sector_query = text("""
        SELECT  m.name as Sector
        ,       w.ticker
        ,       w.rs_rating
        ,       w.weekly_return
        ,       w.is_above_200ma
        FROM    price_weekly w
        INNER JOIN stock_master m ON w.ticker = m.ticker
        WHERE   w.weekly_date = (SELECT MAX(weekly_date) FROM price_weekly)
        AND     m.market_type = 'SECTOR'  -- [중요] 섹터 ETF만 조회
        ORDER BY w.rs_rating DESC;
    """)

    with engine.connect() as conn:
        sector_df = pd.read_sql(sector_query, conn)

    # 섹터 데이터 마크다운 변환
    if not sector_df.empty:
        sector_df['200일선'] = sector_df['is_above_200ma'].apply(lambda x: "O" if x == 1 else "X")
        sector_md = sector_df[['Sector', 'ticker', 'rs_rating', 'weekly_return', '200일선']].to_markdown(index=False)
    else:
        sector_md = "(섹터 데이터 없음)"

    # =========================================================
    # [STEP 2] 개별 주식(Stock) 랭킹 데이터 추출 (Bottom-Up)
    # =========================================================
    stock_query = text("""
        SELECT  m.name
        ,       w.ticker
        ,       d.close as today_close
        ,       ((d.close - d.open) / d.open * 100) as daily_change_pct
        ,       w.rs_rating
        ,       w.is_above_200ma          
        ,       w.deviation_200ma         
        ,       f.fundamental_grade
        ,       f.eps_rating
        ,       fq.net_income
        ,       fq.rev_growth_yoy
        ,       fq.eps_growth_yoy
        ,       fa.roe
        FROM    price_weekly w
        INNER JOIN stock_master m ON w.ticker = m.ticker
        LEFT JOIN stock_fundamentals f ON w.ticker = f.ticker
        INNER JOIN price_daily d
            ON  w.ticker = d.ticker
            AND d.date = (SELECT MAX(date) FROM price_daily)
        LEFT JOIN financial_quarterly fq
            ON  w.ticker = fq.ticker
            AND fq.date = (SELECT MAX(date) FROM financial_quarterly WHERE ticker = w.ticker)
        LEFT JOIN financial_annual fa
            ON  w.ticker = fa.ticker
            AND fa.year = (SELECT MAX(year) FROM financial_annual WHERE ticker = w.ticker)
        WHERE   w.weekly_date = (SELECT MAX(weekly_date) FROM price_weekly)
        AND     m.market_type = 'STOCK'   -- [중요] 개별 기업만
        AND     w.rs_rating >= 80         -- [오닐] 강력한 주도주 조건
        AND     w.is_above_200ma = 1      -- [와인스테인] 2단계 상승 조건
        ORDER BY w.rs_rating DESC LIMIT 20;
    """)

    with engine.connect() as conn:
        stock_df = pd.read_sql(stock_query, conn)

    if stock_df.empty:
        stock_md = "(조건을 만족하는 주도주가 없습니다)"
    else:
        # 데이터 가공
        stock_df['비고'] = stock_df.apply(classify_status, axis=1)
        stock_df['오늘변동'] = stock_df['daily_change_pct'].apply(
            lambda x: f"🔺{x:.1f}%" if x > 0 else (f"▼{x:.1f}%" if x < 0 else "-")
        )

        def format_weinstein_status(row):
            dev = row['deviation_200ma']
            if dev >= 50: return f"과열(이격 {dev}%)"
            if dev >= 0: return f"2단계 유지(이격 {dev}%)"
            return "이탈"

        stock_df['추세상태'] = stock_df.apply(format_weinstein_status, axis=1)

        display_stock_df = stock_df[[
            'ticker', 'name', 'today_close', '오늘변동', 'rs_rating',
            'fundamental_grade', '추세상태', '비고'
        ]]
        stock_md = display_stock_df.to_markdown(index=False)

    # =========================================================
    # [STEP 3] 박스권 돌파 스캐너 (Breakout Scanner) - [NEW]
    # =========================================================
    try:
        breakout_data = scan_breakout_stocks()  # 스캐너 실행

        if breakout_data:
            breakout_df = pd.DataFrame(breakout_data)
            # 리포트에 필요한 컬럼만 선택 및 이름 변경
            breakout_df = breakout_df[['ticker', 'date', 'close', 'box_width_pct', 'vol_spike_pct']]
            breakout_df.columns = ['티커', '날짜', '종가', '박스폭(%)', '거래량급증(%)']
            breakout_md = breakout_df.to_markdown(index=False)
        else:
            breakout_md = "(오늘 검색된 박스권 돌파 종목 없음)"
    except Exception as e:
        logger.error(f"스캐너 실행 실패: {e}")
        breakout_md = "(스캐너 실행 오류)"

    # ---------------------------------------------------------
    # [Prompt] Top-Down + Breakout 전략 통합
    # ---------------------------------------------------------
    prompt = f"""
    # Role: 전설적인 트레이딩 멘토 (AI Investment Strategist)

    # Persona & Tone:
    - 당신은 나의 오랜 투자 멘토입니다. "친구야"라고 부르며 대화하듯 설명해 주세요.
    - 분석은 논리적이어야 하며, **Top-Down(숲을 보고 나무를 보는) 방식**을 사용해야 합니다.

    # 🧠 Your Advisory Board (The Big Five Philosophies):
    1. **윌리엄 오닐:** "주도주는 혼자 가지 않는다. **주도 업종(Sector)에 속한 1등 주식**을 찾아라." (펀더멘털+수급)
    2. **스탠 와인스테인:** "주식이 **2단계 상승 국면**에 있는지, 업종 차트도 살아있는지 확인해라."
    3. **제시 리버모어:** "피봇 포인트를 돌파하는 놈을 사라."
    4. **에드 세이코타:** "추세가 꺾이면 칼같이 잘라라."
    5. **★ 니콜라스 다비스 (New):** "박스권 안에서 웅크리던 주식이 거래량을 동반해 지붕을 뚫을 때(Breakout)가 최고의 기회다."

    # 📊 Market Data:

    ## [GROUP A] Sector Ranking (거시적 흐름 / Top-Down):
    * 설명: 현재 시장 자금이 쏠리고 있는 주도 업종 순위입니다.
    {sector_md}

    ## [GROUP B] Top Stock Ranking (개별 주도주 / Bottom-Up):
    * 설명: RS 80 이상, 200일선 위에 있는 강력한 종목들입니다.
    {stock_md}

    ## [GROUP C] Breakout Candidates (신규 돌파 유망주):
    * 설명: 오랫동안 횡보하다가 오늘 **거래량 폭발 + 박스권 상단 돌파**가 나온 종목들입니다. (니콜라스 다비스 스타일)
    {breakout_md} 

    ---
    # 📝 Report Request:

    ## 1. 🌍 시장 브리핑 (Top-Down Analysis)
    - **[섹터 분석]**: 먼저 `[GROUP A]` 데이터를 보고, 현재 시장의 대장 섹터가 어디인지(예: 반도체가 1등인지, 방어주가 1등인지) 분석해 주세요.
    - **[시장 분위기]**: 대장 섹터의 흐름을 볼 때, 지금이 공격적으로 투자할 때인지 몸을 사릴 때인지 **와인스테인의 관점**에서 진단해 주세요.

    ## 2. 🚀 오늘의 Top Pick (3개 선정)
    - `[GROUP B]`(기존 주도주)와 `[GROUP C]`(신규 돌파주)를 통틀어 가장 매력적인 3종목을 꼽아주세요.
    - **[선정 이유]**: 
      - 기존 주도주라면: "섹터가 받쳐주고 추세가 견고함(와인스테인)"
      - 신규 돌파주라면: "**드디어 박스권을 뚫었음(다비스)**, 이제 막 시세 분출 시작 가능성" 등을 언급해 주세요.
    - **[진입 전략]**: 리버모어의 관점에서 '돌파 매수' 혹은 '눌림목 매수' 가격대를 제안해 주세요.

    ## 3. 📦 박스권 돌파주 집중 분석 (Group C Special)
    - `[GROUP C]`에 종목이 있다면, 니콜라스 다비스에 빙의해서 **"이 종목이 왜 기회인지"** 혹은 **"속임수일 가능성은 없는지(거래량 확인)"** 짧게 코멘트해 주세요. (종목이 없으면 생략)

    ## 4. ⚠️ 리스크 경고 (Seykota's Cut)
    - 이격도가 너무 높거나, 주도 섹터가 아닌데 혼자 급등한 종목(테마주 가능성)에 대해 경고해 주세요.
    - **세이코타의 목소리**로 "추세가 꺾이면 미련 갖지 마라"고 조언해 주세요.

    ## 5. 💡 멘토의 한마디
    - 투자 심리를 다잡을 수 있는 격려의 말을 남겨주세요.
    """

    try:
        # 모델 버전을 2.0-flash로 변경 (최신 고속 모델)
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=prompt
        )
        report_content = response.text

        print("\n" + "=" * 80 + "\n🤖 [Gemini AI 리포트 생성 완료]\n" + "=" * 80)

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        email_subject = f"📈 [Trend Report] {yesterday} 주도 섹터 & 돌파 매매 분석"

        send_email(email_subject, report_content, yesterday)

    except Exception as e:
        logger.error(f"Gemini 분석 실패: {e}")


# ---------------------------------------------------------
# [Email] 이메일 발송 함수
# ---------------------------------------------------------
def send_email(subject, markdown_content, report_date):
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

    if not EMAIL_USER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        print("⚠️ 이메일 환경변수 미설정")
        return

    try:
        html_body = markdown.markdown(markdown_content, extensions=['tables'])

        template_dir = os.path.join(BASE_DIR, "app", "templates")
        try:
            env = Environment(loader=FileSystemLoader(template_dir))
            template = env.get_template('newsletter.html')
            final_html = template.render(date=report_date, body_content=html_body)
        except:
            final_html = f"<html><body><h2>{subject}</h2>{html_body}</body></html>"

        msg = MIMEMultipart('alternative')
        msg['From'] = f"AI Stock Mentor <{EMAIL_USER}>"
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = subject

        msg.attach(MIMEText(final_html, 'html', 'utf-8'))

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)

        print(f"📧 뉴스레터 발송 완료! ({EMAIL_RECEIVER})")

    except Exception as e:
        print(f"❌ 이메일 발송 실패: {e}")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    generate_ai_report()
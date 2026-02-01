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

    # 구글 API 클라이언트 초기화
    client = genai.Client(api_key=GOOGLE_API_KEY)

    # ---------------------------------------------------------
    # [SQL] 4대 전설(오닐, 리버모어, 세이코타, 와인스테인) 데이터 추출
    # ---------------------------------------------------------
    # 조건: RS 80 이상, 주가 200일선 위(와인스테인 필수 조건), ETF 제외
    query = text("""
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
        -- 최신 일간 가격 Join
        INNER JOIN price_daily d
            ON  w.ticker = d.ticker
            AND d.date = (SELECT MAX(date) FROM price_daily)
        -- 최신 분기 실적 Join
        LEFT JOIN financial_quarterly fq
            ON  w.ticker = fq.ticker
            AND fq.date = (SELECT MAX(date) FROM financial_quarterly WHERE ticker = w.ticker)
        -- 최신 연간 실적 Join
        LEFT JOIN financial_annual fa
            ON  w.ticker = fa.ticker
            AND fa.year = (SELECT MAX(year) FROM financial_annual WHERE ticker = w.ticker)
        WHERE   w.weekly_date = (SELECT MAX(weekly_date) FROM price_weekly)
        AND     m.market_type = 'STOCK'   -- [중요] 개별 기업 분석
        AND     w.rs_rating >= 80         -- [오닐] 강력한 주도주
        AND     w.is_above_200ma = 1      -- [와인스테인] 2단계 상승 국면의 필수 전제 (200일선 위)
        ORDER BY w.rs_rating DESC LIMIT 20;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        logger.warning("조건을 만족하는(RS>=80, 200일선 위) 종목이 없습니다.")
        return

    # ---------------------------------------------------------
    # [Python] 데이터 가공 및 포맷팅
    # ---------------------------------------------------------
    df['비고'] = df.apply(classify_status, axis=1)

    # 등락률 포맷팅
    df['오늘변동'] = df['daily_change_pct'].apply(
        lambda x: f"🔺{x:.1f}%" if x > 0 else (f"▼{x:.1f}%" if x < 0 else "-")
    )

    # 와인스테인 2단계 확인용 (이격도 표시)
    def format_weinstein_status(row):
        dev = row['deviation_200ma']
        # 이격도가 0보다 크면 200일선 위에 있는 것 (SQL에서 이미 필터링함)
        if dev >= 50: return f"과열(이격 {dev}%)"
        if dev >= 0: return f"2단계 유지(이격 {dev}%)"
        return "이탈(경고)"

    df['추세상태'] = df.apply(format_weinstein_status, axis=1)

    # AI에게 보낼 컬럼 선택
    display_df = df[[
        'ticker', 'name', 'today_close', '오늘변동', 'rs_rating',
        'fundamental_grade', '추세상태', '비고'
    ]]

    # Markdown 변환
    data_table = display_df.to_markdown(index=False)

    # ---------------------------------------------------------
    # [Prompt] 레전드 4인방 (오닐 + 리버모어 + 세이코타 + 와인스테인)
    # ---------------------------------------------------------
    prompt = f"""
    # Role: 전설적인 트레이딩 멘토 (AI Investment Strategist)

    # Persona & Tone:
    - 당신은 나의 오랜 투자 멘토입니다. 친근하게 "친구야"라고 부르며 대화하듯 설명해 주세요.
    - 분석은 논리적이어야 하지만, **'리스크 관리'**에 대해서는 냉정하고 단호해야 합니다.

    # 🧠 Your Advisory Board (The Big Four Philosophies):
    답변을 작성할 때 다음 4명의 철학을 완벽하게 통합하여 분석하세요.

    1. **🔍 윌리엄 오닐 (William O'Neil):** - "펀더멘털(이익 성장)과 수급(RS)이 모두 받쳐주는 '주도주'인가?" (RS 80 이상, 흑자 선호)

    2. **📈 스탠 와인스테인 (Stan Weinstein):** - "주식이 **'2단계 상승 국면(Stage 2)'**에 있는가?"
       - **절대 원칙:** 30주(200일) 이동평균선 위에 있어야 하며, 주가가 이동평균선을 깨지 않고 타고 올라가야 한다.

    3. **⏱️ 제시 리버모어 (Jesse Livermore):**
       - "단순히 싸다고 사지 마라. **'피봇 포인트'를 돌파**하며 새로운 추세가 시작될 때가 매수 시점이다."

    4. **🛡️ 에드 세이코타 (Ed Seykota):**
       - "추세는 친구다(Trend is your friend)." 하지만 추세가 꺾이면(200일선 이탈 등) 즉시 자라라. 예측하지 말고 대응해라.

    # 📊 Market Data (Top 20 Strongest Stocks):
    {data_table}
    * '추세상태': 스탠 와인스테인의 2단계 확인용 (200일선 위인지, 이격도가 적당한지).
    * '비고': 🟢(흑자/성장-안전), 🟡(적자성장-변동성 주의), 🔴(위험)

    ---
    # 📝 Report Request:

    ## 1. 🌍 시장 추세 브리핑 (Weinstein's Stage Analysis)
    - 상위 종목들이 대체로 **'2단계 상승 국면'**에 안착해 있는지, 아니면 과열(3단계)이나 하락(4단계) 징후가 보이는지 **스탠 와인스테인의 관점**에서 분석해 주세요. 

    ## 2. 🚀 오늘의 Top Pick (3개 선정)
    - **오닐(수급/실적)**이 좋아하고, **와인스테인(2단계 지속)**이 확인되며, **리버모어(돌파)**의 타점이 보이는 최고의 종목 3개를 선정해 주세요.
    - **[선정 이유]**: "이 종목은 RS가 강하고, 200일선 위에서 2단계 상승을 지속 중이야(와인스테인). 실적도 🟢라 오닐 합격점이지."
    - **[진입 전략]**: 리버모어의 관점에서 '돌파 매수' 혹은 '눌림목 매수' 가격대를 제안해 주세요.

    ## 3. ⚠️ 리스크 경고 (Seykota's Cut)
    - **에드 세이코타의 목소리**로 경고해 주세요.
    - 이격도가 지나치게 높거나(과열), 펀더멘털이 🔴인데 기대감만으로 오른 종목이 있다면 "추세가 꺾이면 뒤도 돌아보지 말고 나와야 해"라고 따끔하게 말해주세요.

    ## 4. 💡 멘토의 한마디
    - 투자 심리를 다잡을 수 있는 짧은 격려의 말을 남겨주세요.
    """

    try:
        # 모델명은 사용 가능한 최신 버전으로 설정 (gemini-1.5-flash 권장)
        response = client.models.generate_content(
            model='gemini-1.5-flash',
            contents=prompt
        )
        report_content = response.text

        print("\n" + "=" * 80 + "\n🤖 [Gemini AI 리포트 생성 완료]\n" + "=" * 80)

        # 이메일 발송
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        email_subject = f"📈 [Trend Report] {yesterday} 주도주 분석 (with 4 Legends)"

        send_email(email_subject, report_content, yesterday)

    except Exception as e:
        logger.error(f"Gemini 분석 및 리포트 생성 실패: {e}")


# ... (send_email 함수 및 하단 실행 코드는 기존과 동일 유지) ...
def send_email(subject, markdown_content, report_date):
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

    if not EMAIL_USER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        print("⚠️ 이메일 환경변수(EMAIL_USER 등)가 설정되지 않았습니다.")
        return

    try:
        # Markdown -> HTML 변환 (테이블 스타일 적용)
        html_body = markdown.markdown(markdown_content, extensions=['tables'])

        # HTML 템플릿 로드
        template_dir = os.path.join(BASE_DIR, "app", "templates")
        try:
            env = Environment(loader=FileSystemLoader(template_dir))
            template = env.get_template('newsletter.html')
            final_html = template.render(date=report_date, body_content=html_body)
        except Exception:
            final_html = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                    table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                </style>
            </head>
            <body>
                <h2>{subject}</h2>
                <div>{html_body}</div>
            </body>
            </html>
            """

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
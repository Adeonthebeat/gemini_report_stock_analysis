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
    # None 값은 0으로 처리
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
    # [SQL] 윌리엄 오닐 스타일 데이터 추출
    # ---------------------------------------------------------
    # 조건: RS 80 이상, 주가 200일선 위(와인스테인), ETF 제외(STOCK만)
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
        AND     m.market_type = 'STOCK'   -- [중요] 오닐은 개별 기업을 분석함 (ETF 제외)
        AND     w.rs_rating >= 80         -- [오닐] 강력한 주도주 조건
        AND     w.is_above_200ma = 1      -- [와인스테인] 2단계 상승 국면
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

    # 200일선 상태 표시 (이격도가 너무 크면 경고)
    def format_200ma(row):
        dev = row['deviation_200ma']
        if dev >= 50: return f"과열(이격 {dev}%)"  # 리버모어/세이코타는 과열 주의
        return f"안정(이격 {dev}%)"

    df['이격도상태'] = df.apply(format_200ma, axis=1)

    # AI에게 보낼 컬럼만 선택
    display_df = df[[
        'ticker', 'name', 'today_close', '오늘변동', 'rs_rating',
        'fundamental_grade', '이격도상태', '비고'
    ]]

    # Markdown 변환
    data_table = display_df.to_markdown(index=False)

    # ---------------------------------------------------------
    # [Prompt] 레전드 페르소나 (오닐 + 리버모어 + 세이코타)
    # ---------------------------------------------------------
    prompt = f"""
    # Role: 전설적인 트레이딩 멘토 (Trend Following Expert)

    # Persona & Tone:
    - 당신은 나의 오랜 투자 멘토입니다. 친근하게 "친구야"라고 부르며 대화하듯 설명해 주세요.
    - 하지만 **'자금 관리'와 '손절'**에 대해서는 **에드 세이코타**처럼 냉정하고 단호해야 합니다.
    - 상승 추세에는 **제시 리버모어**처럼 대담한 진입을 권하되, 하락 반전 신호에는 민감하게 반응하세요.

    # 🧠 Your Investment Philosophy (The Big Three):
    답변을 작성할 때 다음 세 명의 철학을 반드시 교차 검증하여 분석하세요.

    1. **🔍 종목 선정 (William O'Neil):** - 펀더멘털(이익 성장)과 수급(RS)이 모두 받쳐주는 '주도주'인가?
       - "RS Rating이 높고(80 이상), 펀더멘털 등급이 좋은 종목에 집중해라."

    2. **⏱️ 매매 타이밍 (Jesse Livermore):**
       - 단순히 싸다고 사지 마라. **'피봇 포인트'를 돌파**하며 새로운 추세가 시작될 때가 매수 시점이다.
       - "달리는 말에 올라타라(Breakout Buy). 수익이 나면 불타기(Pyramiding)를 고려해라."

    3. **🛡️ 청산 및 리스크 관리 (Ed Seykota):**
       - 예측하지 말고 대응해라. "추세는 친구다(Trend is your friend)."
       - 추세가 꺾이면(200일선 이탈 등) 즉시 손절해라. **감정을 배제하고 기계적으로 청산해라.**

    # 📊 Market Data (Top 20 Strongest Stocks):
    {data_table}
    * '비고' 컬럼: 🟢(흑자/성장-안전), 🟡(적자성장-변동성 주의), 🔴(위험)
    * '이격도상태': 200일선과의 거리. 50% 이상이면 단기 과열 가능성 있음.

    ---
    # 📝 Report Request:

    ## 1. 🌍 시장 브리핑 (Trend Check)
    - 상위 종목들의 전반적인 분위기를 보고, 지금이 공격적으로 매수할 때인지 관망할 때인지 **세이코타의 관점**에서 한마디 해주세요.

    ## 2. 🚀 오늘의 Top Pick (3개 선정)
    - 위 리스트 중 **오닐의 기준(펀더멘털+수급)**과 **리버모어의 기준(돌파 가능성)**을 모두 충족하는 최고의 종목 3개를 선정해 주세요.
    - **[선정 이유]**: "이 종목은 RS가 XX로 수급이 강하고, 흑자 구조(🟢)라 오닐이 좋아할 만해."
    - **[진입 전략]**: "현재 가격이 OO불인데, 전고점을 돌파할 때가 리버모어의 매수 타점이야." (구체적 가격 언급)

    ## 3. ⚠️ 리스크 경고 (Risk Management)
    - **세이코타의 목소리**로 경고해 주세요.
    - 이격도가 너무 높거나(과열), 재무 상태가 🔴(위험)인 종목이 있다면, "이건 도박이야. 추세가 꺾이면 바로 나와야 해"라고 따끔하게 말해주세요.

    ## 4. 💡 멘토의 격려
    - 뇌동매매를 참는 것도 실력입니다. 마인드셋을 위한 짧은 명언이나 조언을 남겨주세요.
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
        email_subject = f"📈 [Trend Report] {yesterday} 주도주 분석 (with O'Neil & Livermore)"

        send_email(email_subject, report_content, yesterday)

    except Exception as e:
        logger.error(f"Gemini 분석 및 리포트 생성 실패: {e}")


# ---------------------------------------------------------
# [Email] 이메일 발송 함수
# ---------------------------------------------------------
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
        # 템플릿 파일이 없으면 기본 문자열 템플릿 사용
        try:
            env = Environment(loader=FileSystemLoader(template_dir))
            template = env.get_template('newsletter.html')
            final_html = template.render(date=report_date, body_content=html_body)
        except Exception:
            # 템플릿 파일이 없을 경우 대비한 심플 HTML
            final_html = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                    table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                    h1, h2 {{ color: #2c3e50; }}
                    .footer {{ margin-top: 30px; font-size: 0.8em; color: #777; }}
                </style>
            </head>
            <body>
                <h2>{subject}</h2>
                <div>{html_body}</div>
                <div class="footer">본 메일은 AI 자동 분석 리포트입니다. 투자의 책임은 본인에게 있습니다.</div>
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


# 로컬 테스트용
if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()  # .env 파일 로드
    generate_ai_report()
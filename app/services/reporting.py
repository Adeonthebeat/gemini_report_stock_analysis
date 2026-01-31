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

    if net_income > 0:
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

    # ---------------------------------------------------------
    # [SQL] 레전드들이 분석할 수 있게 '이평선', '이격도' 데이터 추가
    # ---------------------------------------------------------
    query = text("""
        SELECT  m.name
        ,       w.ticker
        ,       d.close as today_close
        ,       ((d.close - d.open) / d.open * 100) as daily_change_pct
        ,       w.rs_rating
        ,       w.is_above_200ma          -- [필수] 스탠 와인스테인 분석용 (200일선 위인가?)
        ,       w.deviation_200ma         -- [필수] 이격도 (너무 높으면 과열)
        ,       f.fundamental_grade
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
        AND     w.rs_rating >= 85         -- (조건을 살짝 완화해서 다양한 케이스 확보)
        AND     w.weekly_return > 0
        AND     (fq.net_income > 0 OR fq.rev_growth_yoy > 15) -- 성장성 최소 조건
        ORDER BY w.rs_rating DESC LIMIT 20;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        logger.warning("분석할 상위 종목이 없습니다.")
        return

    # ---------------------------------------------------------
    # [Python] 데이터 가공
    # ---------------------------------------------------------
    df['비고'] = df.apply(classify_status, axis=1)
    df['오늘변동'] = df['daily_change_pct'].apply(
        lambda x: f"🔺 {x:.1f}%" if x > 0 else (f"▼ {x:.1f}%" if x < 0 else "0.0%")
    )

    # 200일선 돌파 여부 O/X 표시
    df['200일선'] = df['is_above_200ma'].apply(lambda x: "위(상승세)" if x == 1 else "아래(저항)")

    # AI에게 보낼 데이터 정리
    display_df = df[[
        'name', 'ticker', 'today_close', '오늘변동', 'rs_rating',
        '200일선', 'deviation_200ma', 'fundamental_grade', 'roe', '비고'
    ]]

    data_table = display_df.to_markdown(index=False)

    # ---------------------------------------------------------
    # [Prompt] 레전드 페르소나 주입 (사용자 요청 반영)
    # ---------------------------------------------------------
    prompt = f"""
    # Role: 세계 최고의 추세추종 기술적 분석가 (AI 트레이딩 멘토)

    # Persona & Tone:
    - 당신은 나의 친한 투자 친구이자 멘토입니다. 대화하듯이 편안하게 설명해주세요.
    - 하지만 **'리스크 관리'**에 대해서는 타협 없이 엄격하고 단호하게 말해야 합니다.
    - 상승 추세에는 희망적인 어조를 사용하되, 근거 없는 희망 고문은 하지 마세요.
    - 설명은 논리적이고 단계적이어야 합니다.

    # Your 'Dream Team' Advisory Board (철학적 기반):
    당신은 다음 레전드들의 투자 철학을 융합하여 분석해야 합니다. 답변 전에 이들의 관점을 교차 검증하세요.
    1. **제시 리버모어:** 추세가 확인될 때만 매매하며, 수익이 날 때 불타기(피라미딩)를 고려한다.
    2. **에드 세이코타:** "추세는 친구다." 단순함을 유지하고 손절매를 칼같이 지킨다.
    3. **윌리엄 오닐:** CAN SLIM + RS(상대강도)가 높은 주도주에 집중한다.
    4. **스탠 와인스테인:** 주식이 '2단계(상승 국면)'에 있는지 확인한다. (200일선 위인지 중요)
    5. **커티스 페이스 (터틀):** 모멘텀이 붙은 종목을 기계적으로 따라간다.

    # Input Data:
    {data_table}
    * '200일선' 컬럼: 스탠 와인스테인의 2단계 확인용.
    * '비고' 컬럼: 🟢(안전), 🟡(성장주, 변동성 주의), 🔴(위험)

    # Output Request (리포트 작성):

    ## 1. 🌍 시장 브리핑 (간략하게)
    - 지금 시장이 주도주가 달리기 좋은 환경인지 멘토의 관점에서 한마디 해주세요.

    ## 2. 🚀 오늘의 Top Pick (3개 선정)
    - 위 데이터에서 가장 강력한 추세(RS)와 펀더멘털을 가진 종목 3개를 뽑아주세요.
    - **[선정 이유]**: 레전드들의 관점을 인용해서 설명 (예: "이 종목은 와인스테인의 2단계에 진입했고...")
    - **[매매 전략]**: 현재 가격(`today_close`)을 기준으로 신규 진입/추가 매수/관망 의견을 제시하세요.
    - **[주의]**: 🟡(적자성장) 기업인 경우, 리스크를 명확히 경고해주세요.

    ## 3. ⚠️ 리스크 점검 (엄격 모드)
    - 데이터 중 '이격도(deviation_200ma)'가 너무 높거나(과열), 재무가 🔴인 종목에 대해 따끔하게 경고해주세요.

    ## 4. 💡 멘토의 한마디
    - 투자 심리를 다잡을 수 있는 격려의 말을 남겨주세요.
    """

    try:
        response = client.models.generate_content(
            model='gemini-flash-latest',
            contents=prompt
        )
        report_content = response.text

        # ... (이하 저장 및 이메일 발송 코드는 기존과 동일) ...
        print("\n" + "=" * 80 + "\n🤖 [Gemini AI 리포트]\n" + "=" * 80 + "\n" + report_content)

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        email_subject = f"📈 [Morning Brief] {yesterday} 주도주 분석 리포트"
        send_email(email_subject, report_content, yesterday)

    except Exception as e:
        logger.error(f"Gemini 분석 오류: {e}")


# ... (send_email 함수는 그대로 유지) ...

def send_email(subject, markdown_content, report_date):
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

    if not EMAIL_USER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        print("⚠️ 이메일 설정 누락")
        return

    try:
        # 마크다운 -> HTML 변환
        html_body = markdown.markdown(markdown_content, extensions=['tables'])

        template_dir = os.path.join(BASE_DIR, "app", "templates")
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template('newsletter.html')

        final_html = template.render(
            date=report_date,
            body_content=html_body
        )

        msg = MIMEMultipart('alternative')
        msg['From'] = f"AdeStock Bot <{EMAIL_USER}>"
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = subject

        msg.attach(MIMEText(final_html, 'html', 'utf-8'))

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)

        print(f"📧 뉴스레터 발송 완료!")

    except Exception as e:
        print(f"❌ 이메일 발송 실패: {e}")
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import markdown
import pandas as pd
from google import genai
from google.api_core import exceptions
from datetime import datetime, timedelta
from jinja2 import Environment, FileSystemLoader
from prefect import task, get_run_logger
from sqlalchemy import text
from tabulate import tabulate
from dotenv import load_dotenv

# [재시도 로직용 라이브러리]
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

# [사용자 설정] 환경에 맞게 유지
from app.core.database import get_engine
from app.core.config import GOOGLE_API_KEY, BASE_DIR


# ---------------------------------------------------------
# 1. [Scanner] 3개월 우상향 실적주 스캐닝 (변경됨)
# ---------------------------------------------------------
def scan_steady_growth_stocks():
    """
    3개월간 꾸준히 오르고(우상향) 실적이 좋은 종목 스캐닝
    조건:
    1. 3개월(60거래일) 수익률 > 5% (최소한의 상승세)
    2. 현재 주가 > 60일 이동평균선 (추세 유지)
    3. 실적: 순이익 흑자 + (매출 성장 OR EPS 성장)
    """
    engine = get_engine()

    query = text("""
    WITH price_metrics AS (
        SELECT 
            d.ticker,
            d.date,
            d.close,
            -- 3개월 전 종가 (약 60 거래일 전)
            LAG(d.close, 60) OVER (PARTITION BY d.ticker ORDER BY d.date) as close_3m_ago,
            -- 60일 이동평균선 (중기 추세선)
            AVG(d.close) OVER (PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as ma_60
        FROM price_daily d
        JOIN stock_master m ON d.ticker = m.ticker
        WHERE m.market_type = 'STOCK' 
    ),
    latest_price AS (
        SELECT * FROM price_metrics
        WHERE date = (SELECT MAX(date) FROM price_daily)
    ),
    latest_finance AS (
        -- 종목별 최신 재무 데이터 추출
        SELECT f.*
        FROM financial_quarterly f
        JOIN (
            SELECT ticker, MAX(date) as max_date 
            FROM financial_quarterly 
            GROUP BY ticker
        ) recent ON f.ticker = recent.ticker AND f.date = recent.max_date
    )
    SELECT 
        p.ticker,
        m.name,
        p.close,
        ROUND(CAST((p.close - p.close_3m_ago) / p.close_3m_ago * 100 AS numeric), 1) as return_3m_pct,
        f.net_income,
        f.rev_growth_yoy,
        f.eps_growth_yoy
    FROM latest_price p
    JOIN stock_master m ON p.ticker = m.ticker
    JOIN latest_finance f ON p.ticker = f.ticker
    WHERE 
        p.close_3m_ago IS NOT NULL
        AND p.close >= p.close_3m_ago * 1.20  -- 3개월간 최소 20% 이상 상승
        AND p.close > p.ma_60                 -- 60일 이평선 위 (추세 살아있음)
        AND f.net_income > 0                  -- 흑자 기업
        AND (f.rev_growth_yoy > 0 OR f.eps_growth_yoy > 0) -- 성장 기업 (매출 혹은 이익 성장)
    ORDER BY return_3m_pct DESC
    LIMIT 10;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        print("🔍 [Scanner] 조건에 맞는 실적 우상향 종목이 없습니다.")
        return []

    print(f"\n🚀 [Scanner] 실적 기반 우상향 종목 발견: {len(df)}개")
    # 콘솔 확인용 출력
    print(tabulate(df[['name', 'close', 'return_3m_pct', 'rev_growth_yoy']],
                   headers=['종목명', '종가', '3개월수익률(%)', '매출성장(%)'],
                   tablefmt='psql', showindex=False))

    return df.to_dict('records')


# ---------------------------------------------------------
# 2. [Helper] 보조 함수들
# ---------------------------------------------------------
def classify_status(row):
    """재무 데이터를 기반으로 신호등 이모지 반환"""
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


@retry(
    wait=wait_random_exponential(multiplier=2, min=10, max=120),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type(exceptions.ResourceExhausted)
)
def generate_content_safe(client, model_name, contents):
    """Gemini API 호출 시 429 에러 자동 재시도"""
    print(f"🤖 API 호출 시도 중... (Model: {model_name})")
    response = client.models.generate_content(
        model=model_name,
        contents=contents
    )
    return response.text


def send_email(subject, markdown_content, report_date):
    """이메일 발송 함수"""
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

    if not EMAIL_USER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        print("⚠️ 이메일 환경변수가 설정되지 않아 발송을 건너뜁니다.")
        return

    try:
        html_body = markdown.markdown(markdown_content, extensions=['tables'])
        try:
            template_dir = os.path.join(BASE_DIR, "app", "templates")
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


# ---------------------------------------------------------
# 3. [Main Task] AI 리포트 생성 및 발송
# ---------------------------------------------------------
@task(name="Generate-AI-Report")
def generate_ai_report():
    try:
        logger = get_run_logger()
    except:
        import logging
        logger = logging.getLogger("LocalRun")

    engine = get_engine()

    if not GOOGLE_API_KEY:
        logger.error("GOOGLE_API_KEY가 설정되지 않았습니다.")
        return

    client = genai.Client(api_key=GOOGLE_API_KEY)

    # --- [STEP 1] 섹터 데이터 (Top-Down) ---
    sector_query = text("""
            SELECT  m.name as "Sector", w.ticker, w.rs_rating, w.weekly_return, w.is_above_200ma
            FROM    price_weekly w
            INNER JOIN stock_master m ON w.ticker = m.ticker
            WHERE   w.weekly_date = (SELECT MAX(weekly_date) FROM price_weekly)
            AND     m.market_type = 'SECTOR'
            ORDER BY w.rs_rating DESC LIMIT 5;  
        """)
    with engine.connect() as conn:
        sector_df = pd.read_sql(sector_query, conn)

    if not sector_df.empty:
        sector_df['200일선'] = sector_df['is_above_200ma'].apply(lambda x: "O" if x == 1 else "X")
        sector_md = sector_df[['Sector', 'rs_rating', 'weekly_return', '200일선']].to_markdown(index=False)
    else:
        sector_md = "(섹터 데이터 없음)"

    # --- [STEP 2] 주도주 데이터 (Bottom-Up) ---
    stock_query = text("""
        SELECT  m.name, w.ticker, d.close as today_close, 
                ((d.close - d.open) / d.open * 100) as daily_change_pct,
                w.rs_rating, w.rs_trend, w.atr_stop_loss, w.is_above_200ma, w.deviation_200ma,
                w.is_vcp, w.is_vol_dry, -- 🚨 [중요] VCP와 거래량 지표를 SELECT에 반드시 추가해야 합니다!
                f.fundamental_grade, fq.net_income, fq.rev_growth_yoy, fq.eps_growth_yoy
        FROM    price_weekly w
        INNER JOIN stock_master m ON w.ticker = m.ticker
        LEFT JOIN stock_fundamentals f ON w.ticker = f.ticker
        INNER JOIN price_daily d ON w.ticker = d.ticker AND d.date = (SELECT MAX(date) FROM price_daily)
        LEFT JOIN financial_quarterly fq ON w.ticker = fq.ticker AND fq.date = (SELECT MAX(date) FROM financial_quarterly WHERE ticker = w.ticker)
        WHERE   w.weekly_date = (SELECT MAX(weekly_date) FROM price_weekly)
        AND     m.market_type = 'STOCK'
        AND     w.rs_rating >= 87
        AND     w.rs_rating <= 95
        AND     w.is_above_200ma = 1
        AND     f.fundamental_grade IN ('A', 'B')
        AND     w.weekly_return > 0
        ORDER BY w.weekly_return DESC LIMIT 10;
    """)
    with engine.connect() as conn:
        stock_df = pd.read_sql(stock_query, conn)

    if stock_df.empty:
        stock_md = "(조건을 만족하는 주도주가 없습니다)"
    else:
        stock_df['비고'] = stock_df.apply(classify_status, axis=1)
        stock_df['오늘변동'] = stock_df['daily_change_pct'].apply(
            lambda x: f"🔺{x:.1f}%" if x > 0 else (f"▼{x:.1f}%" if x < 0 else "-"))

    def format_weinstein_status(row):
        dev = row['deviation_200ma'] or 0
        # [NEW] VCP와 Volume Dry-up이 동시에 뜬 종목은 특수 마킹
        vcp_signal = " ⭐압축완료" if (row.get('is_vcp') == 1 and row.get('is_vol_dry') == 1) else ""

        if dev >= 50: return f"과열({dev}%)" + vcp_signal
        if dev >= 0: return f"2단계({dev}%)" + vcp_signal
        return "이탈"

    stock_df['추세상태'] = stock_df.apply(format_weinstein_status, axis=1)

    # 💡 [NEW] AI가 보고서에 쓸 수 있도록 표에 'RS강도'와 'atr_stop_loss' 컬럼 추가!
    display_stock_df = stock_df[['ticker', 'name', 'today_close', '오늘변동', 'RS강도', '추세상태', 'atr_stop_loss', '비고']]
    # 마크다운 표 헤더(한국어) 설정
    display_stock_df.columns = ['티커', '종목명', '현재가', '일일변동', 'RS강도(추세)', '추세상태', '2-ATR손절선', '비고']
    stock_md = display_stock_df.to_markdown(index=False)

    # --- [STEP 3] ★ 스캐너 통합 ---
    try:
        steady_data = scan_steady_growth_stocks()
        if steady_data:
            steady_df = pd.DataFrame(steady_data)
            steady_df = steady_df[['name', 'close', 'return_3m_pct', 'net_income', 'rev_growth_yoy']]
            steady_df.columns = ['종목명', '종가', '3개월상승(%)', '순이익', '매출성장(%)']
            steady_md = steady_df.to_markdown(index=False)
        else:
            steady_md = "(조건에 맞는 실적 우상향 종목이 없습니다)"
    except Exception as e:
        logger.error(f"스캐너 실행 실패: {e}")
        steady_md = f"(스캐너 실행 오류: {e})"

    # --- [STEP 4] 프롬프트 작성 및 AI 요청 ---
    prompt = f"""
    # Role: 전설적인 트레이딩 멘토 (AI Investment Strategist)
    # Persona: 윌리엄 오닐, 니콜라스 다비스, 마크 미너비니, 터틀 트레이딩의 철학을 융합한 멘토. "친구야"라고 부르며 따뜻하지만 날카롭게 조언.

    # Data Provided:
    ## [A] Sector Ranking (Top-Down):
    {sector_md}

    ## [B] Leading Stocks (Breakout Candidates):
    * 'RS강도(추세)'의 UP 표시는 현재 RS가 4주 평균 이상으로 모멘텀이 살아있음을 뜻한다.
    * '추세상태'에 '⭐압축완료'가 표시된 종목은 VCP(변동성 수축) 패턴과 거래량 고갈이 동시에 발생한 초강력 매수 후보이다.
    * '2-ATR손절선'은 종목 고유의 변동성을 고려한 기계적 리스크 관리 가격이다.
    {stock_md}

    ## [C] Steady Growth Stocks (Fundamentals + 3M Trend):
    * 이 목록은 최근 3개월간 주가가 꾸준히 오르고(우상향), 실적(순이익, 매출성장)이 뒷받침되는 알짜배기 종목들이다.
    {steady_md}

    # Request:
    1. **시장 흐름 (Top-Down & Internal RS):** - [A]의 강한 섹터 내에 속한 [B] 종목이 있다면, "섹터의 수급(Internal RS)을 함께 받고 있는 진짜 주도주"라는 관점에서 분석해줘.
    2. **오늘의 Top Pick (돌파매매 전략):**
       - [B], [C] 목록을 바탕으로 '돌파매매(Breakout)' 관점에서 최우선 5종목을 추천해줘.
       - 베이스(Base) 패턴을 형성한 후 직전 저항선(Pivot Point)을 돌파하는 시점을 **신규 매수 타점(Buy Point)**으로 명시해.
       - 특히 [B] 목록에서 '⭐압축완료' 마크가 있는 종목이 있다면, "매물 소화가 완료되어 폭발 직전인 차트"라는 점을 강조해줘.
    3. **리스크 관리 (Dynamic Stop-Loss):**
       - 두리뭉실한 손절가 대신, 표에 제공된 '2-ATR손절선' 가격을 구체적으로 언급하며, 이 가격을 이탈하면 미련 없이 빠져나올 기계적 손절 플랜을 작성해.
    4. **멘토의 일침:** 손실은 짧게, 수익은 길게 가져가는 돌파매매의 핵심 멘탈리티를 강조해줘.
    """

    print("🤖 AI 리포트 생성 중...")
    try:
        report_content = generate_content_safe(
            client,
            'gemini-flash-lite-latest',
            prompt
        )

        print("\n" + "=" * 60 + "\n[Gemini Report]\n" + "=" * 60)

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        email_subject = f"📈 [Trend Report] {yesterday} 시장 분석 & 실적 우상향주"

        send_email(email_subject, report_content, yesterday)

    except Exception as e:
        logger.error(f"Gemini API 호출 최종 실패: {e}")


# ---------------------------------------------------------
# 4. [Execution] 통합 실행 진입점
# ---------------------------------------------------------
if __name__ == "__main__":
    load_dotenv()
    generate_ai_report()

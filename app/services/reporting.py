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
                w.rs_rating, w.is_above_200ma, w.deviation_200ma,
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
            if dev >= 50: return f"과열({dev}%)"
            if dev >= 0: return f"2단계({dev}%)"
            return "이탈"

        stock_df['추세상태'] = stock_df.apply(format_weinstein_status, axis=1)
        display_stock_df = stock_df[['ticker', 'name', 'today_close', '오늘변동', 'rs_rating', '추세상태', '비고']]
        stock_md = display_stock_df.to_markdown(index=False)

    # --- [STEP 3] ★ 스캐너 통합 (변경된 부분) ---
    try:
        # [변경] 박스권 스캐너 제거 -> 실적 우상향 스캐너 호출
        steady_data = scan_steady_growth_stocks()

        if steady_data:
            steady_df = pd.DataFrame(steady_data)
            # 프롬프트에 넣기 좋게 컬럼 정리
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
    # Persona: 윌리엄 오닐, 제시 리버모어, 스탠 와인스테인, 니콜라스 다비스, 래리 윌리엄스의 철학을 융합한 멘토. "친구야"라고 부르며 따뜻하지만 날카롭게 조언.

    # Data Provided:
    ## [A] Sector Ranking (Top-Down):
    {sector_md}

    ## [B] Leading Stocks (RS 80+):
    {stock_md}

    ## [C] Steady Growth Stocks (Fundamentals + 3M Trend):
    * 이 목록은 최근 3개월간 주가가 꾸준히 오르고(우상향), 실적(순이익, 매출성장)이 뒷받침되는 알짜배기 종목들이다.
    {steady_md}

    # Request:
    1. **시장 브리핑:** [A]를 보고 현재 시장의 돈이 어디로 흐르는지 분석해줘.
    2. **오늘의 Top Pick:** [B]와 [C] 목록을 종합하여, 지금 가장 안정적이면서도 상승 여력이 있는 5종목을 추천해줘.
       - 기술적(차트) 분석과 기본적(실적) 분석을 섞어서 설명해줘.
       - 기술적분석 할 때는 저항선 및 지지선을 활용해서 신규매수타점 / 추가매수타점 / 손절타점을 말해줘
       - 특히 [C] 목록에 있는 종목이라면 "실적이 뒷받침되는 우상향 종목"임을 강조해줘.
    3. **리스크 관리:** 추천한 종목들의 진입 시 주의할 점이나 손절 가이드.
    4. **멘토의 한마디:** 꾸준한 우상향 투자의 중요성에 대한 격려.
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

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import markdown
import pandas as pd
from google import genai
from datetime import datetime, timedelta
from jinja2 import Environment, FileSystemLoader
from prefect import task, get_run_logger, flow
from sqlalchemy import text
from tabulate import tabulate
from dotenv import load_dotenv

# [사용자 설정] app.core 패키지가 없다면 경로에 맞게 수정 필요
from app.core.database import get_engine
from app.core.config import GOOGLE_API_KEY, BASE_DIR

# ---------------------------------------------------------
# 1. [Scanner] 박스권 돌파 종목 스캐닝 함수
# ---------------------------------------------------------
def scan_breakout_stocks():
    """
    횡보 후 거래량 실린 상승(박스권 돌파) 종목 스캐닝
    """
    engine = get_engine()

    query = text("""
    WITH market_data AS (
        SELECT 
            d.ticker,
            d.date,
            d.close,
            d.volume,
            MAX(d.high) OVER(PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) as box_high,
            MIN(d.low) OVER(PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) as box_low,
            AVG(d.volume) OVER(PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as avg_vol_20,
            COUNT(d.close) OVER(PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) as data_count
        FROM price_daily d
        JOIN stock_master m ON d.ticker = m.ticker
        WHERE m.market_type = 'STOCK' 
    ),
    latest_data AS (
        SELECT * FROM market_data
        WHERE date = (SELECT MAX(date) FROM price_daily)
    )
    SELECT 
        ticker,
        date,
        close,
        box_high,
        ROUND(CAST((box_high - box_low) / box_low * 100 AS numeric), 1) as box_width_pct,
        ROUND(CAST(volume / avg_vol_20 * 100 AS numeric), 0) as vol_spike_pct,
        data_count
    FROM latest_data
    WHERE 
          data_count >= 60
      AND (box_high - box_low) / box_low <= 0.20
      AND close > box_high
      AND volume >= avg_vol_20 * 3.0
      AND (close * volume) > 1000000 
    ORDER BY vol_spike_pct DESC;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        print("🔍 [Scanner] 조건에 맞는 종목이 없습니다.")
        return []

    print(f"\n🚀 [Scanner] 박스권 돌파 종목 발견: {len(df)}개")
    # 콘솔 확인용 출력
    print(tabulate(df[['ticker', 'date', 'close', 'box_width_pct', 'vol_spike_pct']], 
                   headers=['티커', '날짜', '종가', '박스권폭(%)', '거래량급증(%)'], 
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

def send_email(subject, markdown_content, report_date):
    """이메일 발송 함수"""
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

    if not EMAIL_USER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        print("⚠️ 이메일 환경변수(EMAIL_USER 등)가 설정되지 않아 발송을 건너뜁니다.")
        return

    try:
        html_body = markdown.markdown(markdown_content, extensions=['tables'])
        
        # 템플릿 로드 시도, 실패시 기본 HTML 사용
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
    # 로거 설정 (Prefect 컨텍스트가 없으면 일반 print 사용)
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
        SELECT  m.name as Sector, w.ticker, w.rs_rating, w.weekly_return, w.is_above_200ma
        FROM    price_weekly w
        INNER JOIN stock_master m ON w.ticker = m.ticker
        WHERE   w.weekly_date = (SELECT MAX(weekly_date) FROM price_weekly)
        AND     m.market_type = 'SECTOR'
        ORDER BY w.rs_rating DESC LIMIT 10;
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
        AND     w.rs_rating >= 80
        AND     w.is_above_200ma = 1
        ORDER BY w.rs_rating DESC LIMIT 20;
    """)
    with engine.connect() as conn:
        stock_df = pd.read_sql(stock_query, conn)

    if stock_df.empty:
        stock_md = "(조건을 만족하는 주도주가 없습니다)"
    else:
        stock_df['비고'] = stock_df.apply(classify_status, axis=1)
        stock_df['오늘변동'] = stock_df['daily_change_pct'].apply(lambda x: f"🔺{x:.1f}%" if x > 0 else (f"▼{x:.1f}%" if x < 0 else "-"))
        
        def format_weinstein_status(row):
            dev = row['deviation_200ma'] or 0
            if dev >= 50: return f"과열({dev}%)"
            if dev >= 0: return f"2단계({dev}%)"
            return "이탈"
        
        stock_df['추세상태'] = stock_df.apply(format_weinstein_status, axis=1)
        display_stock_df = stock_df[['ticker', 'name', 'today_close', '오늘변동', 'rs_rating', '추세상태', '비고']]
        stock_md = display_stock_df.to_markdown(index=False)

    # --- [STEP 3] ★ 스캐너 통합 (Breakout Scanner) ---
    try:
        # 여기서 위에 정의한 함수를 직접 호출합니다.
        breakout_data = scan_breakout_stocks()

        if breakout_data:
            breakout_df = pd.DataFrame(breakout_data)
            breakout_df = breakout_df[['ticker', 'date', 'close', 'box_width_pct', 'vol_spike_pct']]
            breakout_df.columns = ['티커', '날짜', '종가', '박스폭(%)', '거래량급증(%)']
            breakout_md = breakout_df.to_markdown(index=False)
        else:
            breakout_md = "(오늘 검색된 박스권 돌파 종목 없음 - AI가 시장 상황만 분석합니다)"
    except Exception as e:
        logger.error(f"스캐너 실행 실패: {e}")
        breakout_md = f"(스캐너 실행 오류: {e})"

    # --- [STEP 4] 프롬프트 작성 및 AI 요청 ---
    prompt = f"""
    # Role: 전설적인 트레이딩 멘토 (AI Investment Strategist)
    # Persona: 윌리엄 오닐, 스탠 와인스테인, 니콜라스 다비스의 철학을 가진 멘토. "친구야"라고 부르며 통찰력 있게 조언.
    
    # Data Provided:
    ## [A] Sector Ranking (Top-Down):
    {sector_md}

    ## [B] Leading Stocks (RS 80+):
    {stock_md}

    ## [C] Breakout Candidates (Today's Scanner):
    {breakout_md}

    # Request:
    1. **시장 브리핑:** [A]를 보고 주도 섹터 파악 및 시장 공격/방어 여부 판단.
    2. **오늘의 Top Pick:** [B]와 [C] 중 가장 매력적인 3종목 선정 및 이유 (와인스테인/다비스 관점).
       - 만약 [C]에 종목이 없다면, [B] 위주로 추천하되 "오늘은 돌파 종목이 없으니 무리하지 말라"고 조언.
    3. **리스크 관리:** 과열 종목 경고.
    4. **멘토의 한마디:** 투자 심리 케어.
    """

    print("🤖 AI 리포트 생성 중...")
    try:
        # 현재 시점 기준 1.5 Flash의 가장 성능 좋은 최신 안정화 버전
        response = client.models.generate_content(
            model='gemini-1.5-flash-002', 
            contents=prompt
        )
        report_content = response.text
        
        print("\n" + "=" * 60 + "\n[Gemini Report]\n" + "=" * 60)
        # print(report_content) # 콘솔이 너무 길어지면 주석 처리

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        email_subject = f"📈 [Trend Report] {yesterday} 시장 분석 & 돌파 종목"
        
        send_email(email_subject, report_content, yesterday)

    except Exception as e:
        logger.error(f"Gemini API 호출 실패: {e}")


# ---------------------------------------------------------
# 4. [Execution] 통합 실행 진입점
# ---------------------------------------------------------
if __name__ == "__main__":
    load_dotenv()
    
    # 1. 스캐너만 따로 테스트하고 싶다면 아래 주석 해제
    # scan_breakout_stocks()
    
    # 2. 전체 리포트 생성 프로세스 실행 (스캐너 포함)
    generate_ai_report()

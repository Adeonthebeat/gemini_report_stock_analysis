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
from dotenv import load_dotenv
import yfinance as yf
import traceback
from google.genai import errors

# [재시도 로직용 라이브러리]
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

# [사용자 설정] 환경에 맞게 유지
from app.core.database import get_engine
from app.core.config import GOOGLE_API_KEY, BASE_DIR


# ---------------------------------------------------------
# 1. [Scanner - C] 실적 고성장 & 모멘텀 주도주 스캐닝 (+ RS 가속도 추가)
# ---------------------------------------------------------
def scan_steady_growth_stocks():
    engine = get_engine()

    query = text("""
        WITH daily_stats AS (
            SELECT 
                d.ticker, d.date, d.close, d.volume,
                LAG(d.close, 60) OVER (PARTITION BY d.ticker ORDER BY d.date) as close_3m_ago,
                LAG(d.close, 5) OVER (PARTITION BY d.ticker ORDER BY d.date) as close_1w_ago,
                AVG(d.close) OVER (PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as ma_60,
                AVG(d.volume) OVER (PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as avg_vol_60,
                MAX(d.close) OVER (PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) as high_52w
            FROM price_daily d
            JOIN stock_master m ON d.ticker = m.ticker
            WHERE m.market_type = 'STOCK' 
        ),
        latest_stats AS (
            SELECT * FROM daily_stats WHERE date = (SELECT MAX(date) FROM price_daily)
        ),
        latest_finance AS (
            SELECT f.* FROM financial_quarterly f
            JOIN (
                SELECT ticker, MAX(date) as max_date 
                FROM financial_quarterly GROUP BY ticker
            ) recent ON f.ticker = recent.ticker AND f.date = recent.max_date
        ),
        -- 🌟 [RS 가속도 로직 추가] 1주 전 RS 점수 가져오기
        weekly_data AS (
            SELECT ticker, weekly_date, atr_stop_loss, rs_rating,
                    LAG(rs_rating, 1) OVER (PARTITION BY ticker ORDER BY weekly_date) as rs_1w_ago
            FROM price_weekly
        ),
        latest_weekly AS (
            SELECT * FROM weekly_data WHERE weekly_date = (SELECT MAX(weekly_date) FROM price_weekly)
        )
        SELECT 
            s.ticker, m.name, s.close,
            ROUND(CAST((s.close - s.close_3m_ago) / NULLIF(s.close_3m_ago, 0) * 100 AS numeric), 1) as return_3m_pct,
            ROUND(CAST((s.close - s.close_1w_ago) / NULLIF(s.close_1w_ago, 0) * 100 AS numeric), 1) as return_1w_pct,
            ROUND(CAST((s.close / NULLIF(s.high_52w, 0)) * 100 AS numeric), 1) as pct_to_52w_high, 
            w.atr_stop_loss,
            w.rs_rating,
            (w.rs_rating - w.rs_1w_ago) as rs_accel, -- 🌟 RS 가속도 계산
            f.net_income, f.rev_growth_yoy, f.eps_growth_yoy,
            sf.roe -- 🌟 stock_fundamentals 테이블에서 roe 출력
        FROM latest_stats s
        JOIN stock_master m ON s.ticker = m.ticker
        JOIN latest_finance f ON s.ticker = f.ticker
        LEFT JOIN latest_weekly w ON s.ticker = w.ticker
        INNER JOIN stock_fundamentals sf ON s.ticker = sf.ticker
        WHERE 
            s.close_3m_ago IS NOT NULL AND s.close_1w_ago IS NOT NULL
            AND s.close >= 10 AND s.close_3m_ago >= 5 AND s.avg_vol_60 >= 200000
            AND f.net_income > 0 AND f.rev_growth_yoy >= 15 
            AND f.eps_growth_yoy >= 15
            AND sf.roe > 10
            AND s.close >= s.close_3m_ago * 1.15 
            AND s.close > s.ma_60
            AND sf.fundamental_grade IN ('A')
        ORDER BY rs_rating DESC, rs_accel DESC
        LIMIT 10;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return [] if df.empty else df.to_dict('records')


# ---------------------------------------------------------
# 2. [NEW Scanner - D] 실적 기반 20일선 눌림목 (우량주 숨고르기)
# ---------------------------------------------------------
def scan_pullback_stocks():
    """심신을 지켜주는 흑자/고성장 20일선 눌림목 매매 로직"""
    engine = get_engine()

    query = text("""
       WITH daily_stats AS (
            SELECT 
                d.ticker, d.date, d.close, d.volume,
                AVG(d.close) OVER (PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma_20,
                AVG(d.close) OVER (PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as ma_60,
                AVG(d.volume) OVER (PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_vol_20
            FROM price_daily d
            JOIN stock_master m ON d.ticker = m.ticker
            WHERE m.market_type = 'STOCK' 
        ),
        latest_stats AS (
            SELECT * FROM daily_stats WHERE date = (SELECT MAX(date) FROM price_daily)
        ),
        latest_weekly AS (
            SELECT ticker, atr_stop_loss, rs_rating FROM price_weekly 
            WHERE weekly_date = (SELECT MAX(weekly_date) FROM price_weekly)
        ),
        -- 🌟 [NEW] 재무제표 최신 데이터 가져오기 (토 기운 보강)
        latest_finance AS (
            SELECT f.* FROM financial_quarterly f
            JOIN (
                SELECT ticker, MAX(date) as max_date 
                FROM financial_quarterly GROUP BY ticker
            ) recent ON f.ticker = recent.ticker AND f.date = recent.max_date
        )
        SELECT 
            s.ticker, m.name, s.close,
            ROUND(CAST(s.ma_20 AS numeric), 2) as ma_20,
            ROUND(CAST((s.close / s.ma_20) * 100 AS numeric), 1) as pct_to_ma20,
            w.atr_stop_loss, 
            w.rs_rating,
            f.net_income, f.rev_growth_yoy, f.eps_growth_yoy, -- [NEW] 재무 데이터 추출
            sf.roe -- 🌟 stock_fundamentals 테이블에서 roe 출력 추가
        FROM latest_stats s
        JOIN stock_master m ON s.ticker = m.ticker
        LEFT JOIN latest_weekly w ON s.ticker = w.ticker
        JOIN latest_finance f ON s.ticker = f.ticker      -- 🌟 [NEW] 재무 테이블 조인
        INNER JOIN stock_fundamentals sf ON s.ticker = sf.ticker -- 🌟 roe를 가져오기 위해 테이블 조인 추가
        WHERE 
            s.close >= 10
        
            -- [차트 & 거래량 조건: 20일선 눌림목]
            AND s.ma_20 > s.ma_60                          -- 중기 우상향 정배열
            AND s.close > s.ma_60                          -- 60일선 위 (추세 생존)
            AND (s.close / s.ma_20) BETWEEN 0.98 AND 1.02 -- 20일선 근접 (-2% ~ +2% 이격)
            AND s.volume < s.avg_vol_20 * 0.7              -- 거래량 30% 이상 급감 (매도세 고갈)
        
            -- 🌟 [기본적 분석 조건: 100억 멘탈 보호용 콘크리트 바닥]
            AND f.net_income > 0                          -- 무조건 흑자 기업일 것
            AND f.rev_growth_yoy >= 10 
            AND f.eps_growth_yoy >= 10 -- 매출이나 EPS가 최소 10% 이상 성장 중일 것
            AND sf.roe > 10                                -- 🌟 roe 0 초과 조건 추가 완료
        
        ORDER BY w.rs_rating DESC NULLS LAST
        LIMIT 10;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return [] if df.empty else df.to_dict('records')

# ---------------------------------------------------------
# 3. [Helper] 보조 함수들
# ---------------------------------------------------------
def classify_status(row):
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


# 예외 타입을 errors.APIError 로 변경하여 503, 429 에러 등을 모두 재시도하게 만듭니다.
@retry(
    wait=wait_random_exponential(multiplier=2, min=10, max=120), 
    stop=stop_after_attempt(5), # 너무 많이 시도하면 무한 대기할 수 있으니 5~10회로 조절
    retry=retry_if_exception_type(errors.APIError)
)
def generate_content_safe(client, model_name, contents):
    print(f"🤖 API 호출 시도 중... (Model: {model_name})")
    return client.models.generate_content(model=model_name, contents=contents).text


def send_email(subject, markdown_content, report_date):
    EMAIL_USER, EMAIL_PASSWORD, EMAIL_RECEIVER = os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASSWORD"), os.getenv(
        "EMAIL_RECEIVER")
    if not EMAIL_USER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        print("⚠️ 이메일 환경변수 누락. 발송 건너뜀.")
        return
    try:
        html_body = markdown.markdown(markdown_content, extensions=['tables'])
        try:
            env = Environment(loader=FileSystemLoader(os.path.join(BASE_DIR, "app", "templates")))
            final_html = env.get_template('newsletter.html').render(date=report_date, body_content=html_body)
        except:
            final_html = f"<html><body><h2>{subject}</h2>{html_body}</body></html>"
        msg = MIMEMultipart('alternative')
        msg['From'], msg['To'], msg['Subject'] = f"AI Stock Mentor <{EMAIL_USER}>", EMAIL_RECEIVER, subject
        msg.attach(MIMEText(final_html, 'html', 'utf-8'))
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"📧 뉴스레터 발송 완료! ({EMAIL_RECEIVER})")
    except Exception as e:
        print(f"❌ 이메일 발송 실패: {e}")


# ---------------------------------------------------------
# 4. [Main Task] AI 리포트 생성 및 발송
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
        logger.error("GOOGLE_API_KEY 누락")
        return
    client = genai.Client(api_key=GOOGLE_API_KEY)

    # --- [STEP 1: A] 섹터 데이터 ---
    with engine.connect() as conn:
        sector_df = pd.read_sql(text("""
            SELECT m.name as "Sector", w.ticker, w.rs_rating, w.weekly_return, w.is_above_200ma
            FROM price_weekly w JOIN stock_master m ON w.ticker = m.ticker
            WHERE w.weekly_date = (SELECT MAX(weekly_date) FROM price_weekly) AND m.market_type = 'SECTOR'
            ORDER BY w.rs_rating DESC LIMIT 5;  
        """), conn)
    sector_md = sector_df[['Sector', 'rs_rating', 'weekly_return']].to_markdown(
        index=False) if not sector_df.empty else "(데이터 없음)"

    # --- [STEP 2: B] 주도주 데이터 (+ RS 가속도) ---
    stock_query = text("""
        WITH weekly_lag AS (
            SELECT ticker, weekly_date, rs_rating, rs_trend, atr_stop_loss, is_above_200ma, deviation_200ma, is_vcp, is_vol_dry, weekly_return,
                    LAG(rs_rating, 1) OVER (PARTITION BY ticker ORDER BY weekly_date) as rs_1w_ago,
                    LAG(rs_rating, 2) OVER (PARTITION BY ticker ORDER BY weekly_date) as rs_2w_ago
            FROM price_weekly
        ),
        current_weekly AS (
            SELECT * FROM weekly_lag WHERE weekly_date = (SELECT MAX(weekly_date) FROM price_weekly)
        )
        SELECT  m.name, w.ticker, d.close as today_close, 
                ((d.close - d.open) / d.open * 100) as daily_change_pct,
                w.rs_rating, 
                (w.rs_rating - w.rs_1w_ago) as rs_accel, -- 🌟 RS 가속도
                w.rs_trend, w.atr_stop_loss, w.deviation_200ma, w.is_vcp, w.is_vol_dry,
                f.fundamental_grade, 
                f.roe, -- 🌟 roe 출력 추가
                fq.net_income, fq.rev_growth_yoy, fq.eps_growth_yoy
        FROM current_weekly w
        INNER JOIN stock_master m ON w.ticker = m.ticker
        LEFT JOIN stock_fundamentals f ON w.ticker = f.ticker
        INNER JOIN price_daily d ON w.ticker = d.ticker AND d.date = (SELECT MAX(date) FROM price_daily)
        LEFT JOIN financial_quarterly fq ON w.ticker = fq.ticker AND fq.date = (SELECT MAX(date) FROM financial_quarterly WHERE ticker = w.ticker)
        WHERE w.rs_rating >= 80
        AND (w.rs_rating - w.rs_1w_ago) >= 1 -- 🌟 가속도가 3점 이상 붙은 진짜배기만 필터링
        AND w.is_above_200ma = 1 
        AND f.fundamental_grade IN ('A') 
        AND w.weekly_return > 0
        AND m.market_type = 'STOCK'
        AND f.roe > 0 -- 🌟 roe가 0보다 큰 조건 추가
        ORDER BY rs_rating DESC, rs_accel DESC LIMIT 10;
    """)
    with engine.connect() as conn:
        stock_df = pd.read_sql(stock_query, conn)

    if not stock_df.empty:
        stock_df['비고'] = stock_df.apply(classify_status, axis=1)
        stock_df['오늘변동'] = stock_df['daily_change_pct'].apply(
            lambda x: f"🔺{x:.1f}%" if x > 0 else (f"▼{x:.1f}%" if x < 0 else "-"))

        def format_w(row):
            dev = row['deviation_200ma'] or 0
            vcp = " ⭐압축완료" if (row.get('is_vcp') == 1 and row.get('is_vol_dry') == 1) else ""
            if dev >= 50: return f"과열({dev}%)" + vcp
            if dev >= 0: return f"2단계({dev}%)" + vcp
            return "이탈"

        stock_df['추세상태'] = stock_df.apply(format_w, axis=1)
        display_stock_df = stock_df[
            ['ticker', 'name', 'today_close', '오늘변동', 'rs_rating', 'rs_accel', 'rs_trend', '추세상태', 'atr_stop_loss',
             '비고']]
        display_stock_df.columns = ['티커', '종목명', '현재가', '일일변동', 'RS점수', 'RS가속도', 'RS강도(추세)', '추세상태', '2-ATR손절선', '비고']
        stock_md = display_stock_df.to_markdown(index=False)
    else:
        stock_md = "(조건 만족 주도주 없음)"

    # --- [STEP 3: C] 스캐너 통합 (+ RS 가속도) ---
    try:
        steady_data = scan_steady_growth_stocks()
        if steady_data:
            steady_df = pd.DataFrame(steady_data)[
                ['ticker', 'name', 'close', 'return_3m_pct', 'return_1w_pct', 'rev_growth_yoy', 'eps_growth_yoy',
                 'rs_rating', 'rs_accel', 'atr_stop_loss']]
            steady_df.columns = ['티커', '종목명', '종가', '3개월수익률', '1주일수익률', '매출성장', 'EPS성장', 'RS점수', 'RS가속도', '2-ATR손절선']
            steady_md = steady_df.to_markdown(index=False)
        else:
            steady_md = "(조건 만족 스윙 주도주 없음)"
    except Exception as e:
        logger.error(f"스캐너 C 실패: {e}")
        steady_md = "(데이터 로드 실패)"

    # --- [STEP 4: D] ★ 신규: 눌림목 데이터 ---
    try:
        pullback_data = scan_pullback_stocks()
        if pullback_data:
            pullback_df = pd.DataFrame(pullback_data)[
                ['ticker', 'name', 'close', 'ma_20', 'pct_to_ma20', 'rs_rating', 'atr_stop_loss']]
            pullback_df.columns = ['티커', '종목명', '종가(현재)', '20일선가격', '20일선대비이격(%)', 'RS점수', '2-ATR손절선']
            pullback_md = pullback_df.to_markdown(index=False)
        else:
            pullback_md = "(현재 20일선 이격도 및 거래량 감소 조건을 만족하는 눌림목 종목이 없습니다)"
    except Exception as e:
        logger.error(f"스캐너 D 실패: {e}")
        pullback_md = "(눌림목 데이터 로드 실패)"

        # --- [STEP 5] 프롬프트 작성 및 AI 요청 (7종목 추천) ---
    prompt = f"""
        # Role: 전설적인 트레이딩 멘토 (AI Investment Strategist)
        # Persona: 월스트리트의 전설 '제시 리버모어(Jesse Livermore)'. 
        오직 추세에 순응하고, 손실은 짧게 자르며 이익은 길게 가져가는 철학. 
        감정을 철저히 배제하고 시장의 가격 움직임(Price Action)과 거래량만을 믿으며, 단호하고 냉철한 어조로 조언해.

        # Data Provided:
        ## [A] Sector Ranking (Top-Down):
        {sector_md}

        ## [B] Leading Stocks (Breakout Candidates):
        * 'RS가속도'는 지난주 대비 RS 점수가 얼마나 급등했는지를 보여주는 폭발력 지표이다.
        * '추세상태'의 '⭐'는 VCP 패턴 완성을 의미한다.
        {stock_md}

        ## [C] High-Growth Momentum Stocks:
        * 실적 고성장과 듀얼 모멘텀, 그리고 'RS가속도'가 동반된 급등 패턴 종목.
        {steady_md}

        ## [D] Pullback Candidates (20MA Touch):
        * 우상향 추세(20일선 > 60일선) 속에서 단기 조정을 받아 20일 이동평균선에 근접(±2%)하고 거래량이 급감한 종목들이다.
        * 심리적 안정감과 손익비가 매우 뛰어난 눌림목 매매(Pullback) 후보들이다.
        {pullback_md}

        # Request:
        1. **시장의 큰 추세:** [A]를 기반으로 현재 자금이 몰리는 주도 섹터의 흐름을 냉철하게 분석해.
        2. **오늘의 주도주 (총 7종목 선정):** - [B], [C] 목록에서 '강한 추세 돌파 및 폭발적인 모멘텀(RS 가속도)'을 보여주는 4종목을 선정해. (달리는 말에 올라타는 전략)
           - [D] 목록에서 '추세 속의 건전한 조정(20일선 지지)'을 보여주는 3종목을 선정해 (총 7종목).
           - 각 종목별로 선정 이유를 가격 움직임과 거래량 중심으로 서술하고, 진입 타점(Buy Point)과 기계적인 손절선(2-ATR)을 명확히 제시해.
           - 특히 손절선에 대해서는 "이 가격을 이탈하면 시장이 틀린 것이 아니라 네가 틀린 것이니 가차 없이 잘라내라"는 뉘앙스를 담아줘.

        [출력 형식 필수 지침]
        * 경고: 어떠한 경우에도 표(Table) 형식이나 마크다운 테이블(|---|)을 사용하지 마세요. 계층형 글머리 기호를 사용하세요.

        [출력 템플릿]
        🚀 **제시 리버모어의 주도주 포착 (돌파 & 눌림목 7선):**

        1. **종목명: [티커] ([전체 종목명])** - [매매 전략 타입: 돌파매매 or 눌림목매매]
            * **선정 이유:** [가격 움직임, RS 가속도, 거래량 감소 등 리버모어 관점의 핵심 이유]
            * **진입 타점 (Buy Point):** [정확한 매수 조건 및 가격]
            * **추가 진입 타점 (Pyramiding):** [추세가 확인된 후 불타기할 타점]
            * **절대 손절선 (2-ATR):** [제공된 2-ATR 손절선 가격 명시 및 단호한 손절 원칙 강조]
    """

    print("🤖 AI 리포트 생성 중...")
    try:
        report_content = generate_content_safe(client, 'gemini-2.5-flash', prompt)
        print("\n" + "=" * 60 + "\n[Gemini Report]\n" + "=" * 60)
        vti_check = yf.download('VTI', period='5d', progress=False, auto_adjust=True)
        target_date_str = vti_check.index[-1].date().strftime('%Y-%m-%d')
        send_email(f"📈 [Trend Report] {target_date_str} 주도주 돌파 & 눌림목 분석", report_content, target_date_str)
    except Exception as e:
        print("\n" + "🚨" * 30)
        print(f"❌ [에러 발생] 원인 파악을 위한 상세 로그:")
        traceback.print_exc()  # 어디서 에러가 났는지 상세히 출력
        print("🚨" * 30 + "\n")
        logger.error(f"Gemini API 호출 최종 실패: {e}")
        raise  # 🌟 핵심! 에러를 던져야 Prefect가 Failed로 인식하고 멈춥니다.


if __name__ == "__main__":
    load_dotenv()
    generate_ai_report()

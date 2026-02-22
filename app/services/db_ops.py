from prefect import task, flow, get_run_logger
from sqlalchemy import text
from app.core.database import get_engine


@task(name="Get-Tickers")
def get_tickers():
    """
    분석 대상 티커와 마켓 타입(STOCK/SECTOR)을 가져옵니다.
    """
    engine = get_engine()
    with engine.connect() as conn:
        # [수정] 테이블명 소문자(stock_master)로 변경
        # market_type 컬럼이 없어도 에러나지 않게 처리하려면 스키마 확인이 필요하지만,
        # 앞서 마이그레이션을 했다고 가정하고 SELECT 합니다.
        try:
            query = text("SELECT ticker, market_type FROM stock_master where 1=1 ")
            result = conn.execute(query).fetchall()
            return [{'ticker': row.ticker, 'market_type': row.market_type} for row in result]
        except Exception:
            # 혹시 market_type 컬럼이 아직 없다면 기본값 처리
            query = text("SELECT ticker FROM stock_master")
            result = conn.execute(query).fetchall()
            return [{'ticker': row.ticker, 'market_type': 'STOCK'} for row in result]


@task(name="Save-Data-To-DB")
def save_to_sqlite(daily_result, weekly_result):
    """
    Supabase(PostgreSQL)에 데이터 저장 (Upsert)
    * 함수 이름은 유지하되 내용은 Postgres 전용입니다.
    """
    logger = get_run_logger()
    engine = get_engine()

    with engine.begin() as conn:
        # 1. 일간 데이터 저장 (소문자 테이블/컬럼)
        # PostgreSQL에서는 DATE 타입에 문자열을 넣으면 자동으로 형변환됩니다.
        conn.execute(text("""
            INSERT INTO price_daily (ticker, date, open, high, low, close, volume)
            VALUES (:ticker, :date, :open, :high, :low, :close, :volume)
            ON CONFLICT(ticker, date) 
            DO UPDATE SET 
                close = EXCLUDED.close, 
                volume = EXCLUDED.volume,
                high = EXCLUDED.high,
                low = EXCLUDED.low
        """), daily_result)

        # 2. 주간 지표 데이터 저장 (소문자 테이블/컬럼)
        conn.execute(text("""
            INSERT INTO price_weekly (
                ticker, weekly_date, weekly_return, rs_value, 
                is_above_200ma, deviation_200ma, is_vcp, is_vol_dry, atr_stop_loss
            )
            VALUES (
                :ticker, :weekly_date, :weekly_return, :rs_value, 
                :is_above_200ma, :deviation_200ma, :is_vcp, :is_vol_dry, :atr_stop_loss
            )
            ON CONFLICT(ticker, weekly_date) 
            DO UPDATE SET 
                rs_value = EXCLUDED.rs_value, 
                is_above_200ma = EXCLUDED.is_above_200ma,
                deviation_200ma = EXCLUDED.deviation_200ma,
                is_vcp = EXCLUDED.is_vcp,
                is_vol_dry = EXCLUDED.is_vol_dry,
                atr_stop_loss = EXCLUDED.atr_stop_loss
        """), weekly_result)

    # (선택) 로그가 너무 많으면 주석 처리
    # logger.info(f"Saved: {daily_result['ticker']}")


@flow(name="Initialize-DB")
def init_db_flow():
    """
    Supabase(PostgreSQL) 테이블 초기화
    * 대소문자 이슈 방지를 위해 모두 소문자로 생성
    * 적절한 데이터 타입(VARCHAR, DATE, BIGINT) 사용
    """
    engine = get_engine()
    with engine.begin() as conn:
        # 1. 종목 마스터 (market_type 추가됨)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_master (
                ticker VARCHAR(20) PRIMARY KEY,
                name VARCHAR(255),
                market_type VARCHAR(20) DEFAULT 'STOCK'
            );
        """))

        # 2. 일간 가격 (DATE 타입, BIGINT 타입 사용)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS price_daily (
                ticker VARCHAR(20),
                date DATE,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume BIGINT,
                PRIMARY KEY (ticker, date)
            );
        """))

        # 3. 주간 데이터 (RS 지표 포함)
        conn.execute(text("""
           CREATE TABLE IF NOT EXISTS price_weekly (
                ticker VARCHAR(20),
                weekly_date DATE,
                weekly_return REAL,
                rs_value REAL, 
                is_above_200ma INTEGER,
                deviation_200ma REAL,
                rs_rating REAL,
                rs_momentum REAL,
                stock_grade VARCHAR(10),
                is_vcp INTEGER DEFAULT 0,       -- [NEW] 변동성 수축 여부
                is_vol_dry INTEGER DEFAULT 0,   -- [NEW] 거래량 고갈 여부
                PRIMARY KEY (ticker, weekly_date)
            );
        """))

        # 4. 재무 기본 정보
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_fundamentals (
                ticker VARCHAR(20) PRIMARY KEY,
                latest_q_date DATE,
                fundamental_grade VARCHAR(10),
                eps_rating REAL,
                updated_at TIMESTAMP
            );
        """))

        # 5. [추가] 리포트용 상세 재무 테이블 (쿼터)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS financial_quarterly (
                ticker VARCHAR(20),
                date DATE,
                net_income BIGINT,
                revenue BIGINT,
                rev_growth_yoy REAL,
                eps_growth_yoy REAL,
                PRIMARY KEY (ticker, date)
            );
        """))

        # 6. [추가] 리포트용 상세 재무 테이블 (연간 - ROE용)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS financial_annual (
                ticker VARCHAR(20),
                year INTEGER,
                net_income BIGINT,
                revenue BIGINT,
                roe REAL,
                PRIMARY KEY (ticker, year)
            );
        """))

    print("✅ Supabase(PostgreSQL) 테이블 초기화 및 스키마 점검 완료")


def get_finished_tickers(target_date_str):
    """
    이미 작업이 완료된(DB에 해당 날짜 데이터가 있는) 티커 목록을 가져옵니다.
    Set 자료형으로 반환하여 검색 속도를 높입니다.
    """
    engine = get_engine()
    
    query = text("SELECT ticker FROM price_daily WHERE date = :date")
    
    with engine.connect() as conn:
        result = conn.execute(query, {"date": target_date_str}).fetchall()
        
    # 예: {'AAPL', 'TSLA', 'NVDA'} 형태의 집합(Set)으로 반환
    return {row[0] for row in result}

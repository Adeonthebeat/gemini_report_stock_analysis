from prefect import task, flow, get_run_logger
from sqlalchemy import text
from app.core.database import get_engine

@task(name="Get-Tickers")
def get_tickers():
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT TICKER FROM STOCK_MASTER"))
        return [row[0] for row in result]

@task(name="Save-Data-To-DB")
def save_to_sqlite(daily_result, weekly_result):
    logger = get_run_logger()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO PRICE_DAILY (TICKER, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
            VALUES (:TICKER, :DATE, :OPEN, :HIGH, :LOW, :CLOSE, :VOLUME)
            ON CONFLICT(TICKER, DATE) DO UPDATE SET CLOSE=excluded.CLOSE, VOLUME=excluded.VOLUME
        """), daily_result)
        conn.execute(text("""
            INSERT INTO PRICE_WEEKLY (TICKER, WEEKLY_DATE, WEEKLY_RETURN, RS_VALUE, IS_ABOVE_200MA, DEVIATION_200MA)
            VALUES (:TICKER, :WEEKLY_DATE, :WEEKLY_RETURN, :RS_VALUE, :IS_ABOVE_200MA, :DEVIATION_200MA)
            ON CONFLICT(TICKER, WEEKLY_DATE) DO UPDATE SET RS_VALUE=excluded.RS_VALUE, IS_ABOVE_200MA=excluded.IS_ABOVE_200MA
        """), weekly_result)
    logger.info(f"Saved: {daily_result['TICKER']}")

@flow(name="Initialize-DB")
def init_db_flow():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS STOCK_MASTER (TICKER TEXT PRIMARY KEY, NAME TEXT)"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS PRICE_DAILY (
                TICKER TEXT, DATE TEXT, OPEN REAL, HIGH REAL, LOW REAL, CLOSE REAL, VOLUME INTEGER,
                PRIMARY KEY (TICKER, DATE))
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS PRICE_WEEKLY (
                TICKER TEXT, WEEKLY_DATE TEXT, WEEKLY_RETURN REAL, RS_VALUE REAL, 
                IS_ABOVE_200MA INTEGER, DEVIATION_200MA REAL,
                PRIMARY KEY (TICKER, WEEKLY_DATE))
        """))
        # 재무 테이블도 함께 초기화
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS STOCK_FUNDAMENTALS (
                TICKER TEXT PRIMARY KEY, LATEST_Q_DATE TEXT, FUNDAMENTAL_GRADE TEXT, 
                EPS_RATING REAL, UPDATED_AT TEXT)
        """))
    print("DB 테이블 초기화 완료")
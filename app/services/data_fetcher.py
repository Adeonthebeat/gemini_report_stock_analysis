import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from prefect import task, get_run_logger
from sqlalchemy import text
from app.core.database import get_engine


@task(name="Check-Market-Update")
def check_market_data_update(benchmark='VTI'):
    logger = get_run_logger()
    engine = get_engine()

    try:
        market_df = yf.download(benchmark, period="5d", progress=False, auto_adjust=False)
        if market_df.empty:
            return False
        latest_market_date = market_df.index[-1].strftime('%Y%m%d')
    except Exception as e:
        logger.error(f"시장 데이터 확인 중 오류: {e}")
        return False

    with engine.connect() as conn:
        query = text("select max(date) from price_daily where ticker = :ticker")
        result = conn.execute(query, {"ticker": benchmark}).scalar()

    if result and result >= latest_market_date:
        logger.info("이미 최신 데이터입니다.")
        return True
    return False


def fetch_combined_data(ticker, benchmark='VTI'):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=500)

    df = yf.download([ticker, benchmark], start=start_date, end=end_date,
                     interval='1d', auto_adjust=False, progress=False)

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col).strip() for col in df.columns.values]

    return df.dropna()
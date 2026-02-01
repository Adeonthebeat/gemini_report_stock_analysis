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


def fetch_combined_data(ticker, market_type='STOCK', benchmark='VTI'):
    end_date = datetime.now()
    # 200일선 등 계산을 위해 넉넉히 2년치
    start_date = end_date - timedelta(days=730)

    print(f"📥 {ticker} ({market_type}) vs {benchmark} 데이터 수집 중...")

    try:
        # 1. 티커와 벤치마크 같이 다운로드
        df = yf.download([ticker, benchmark], start=start_date, end=end_date,
                         interval='1d', auto_adjust=True, progress=False)

        if df.empty:
            return pd.DataFrame()

        # 2. [중요] MultiIndex 컬럼 평탄화 (Price, Ticker) -> Price_Ticker
        # 예: ('Close', 'AAPL') -> 'Close_AAPL'
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [f'{col[0]}_{col[1]}' for col in df.columns]
        else:
            # 티커가 하나만 요청되었거나 구조가 다를 경우 포맷 통일
            # (이 로직을 타면 calculate_metrics에서 Close_VTI를 못 찾아 에러날 수 있으므로 주의)
            df.columns = [f'{col}_{ticker}' for col in df.columns]

        return df.dropna()

    except Exception as e:
        print(f"❌ {ticker} 데이터 수집 실패: {e}")
        return pd.DataFrame()

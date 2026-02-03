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
    # [설정] 오늘 날짜 데이터를 포함하기 위해 내일 날짜까지 범위를 잡습니다.
    end_date = datetime.now() + timedelta(days=1)
    start_date = end_date - timedelta(days=730)

    print(f"📥 {ticker} ({market_type}) vs {benchmark} 데이터 수집 중... (~{end_date.strftime('%Y-%m-%d')})")

    try:
        # 1. 데이터 다운로드
        df = yf.download([ticker, benchmark], start=start_date, end=end_date,
                         interval='1d', auto_adjust=True, progress=False)

        if df.empty:
            return pd.DataFrame()

        # 2. 인덱스(날짜)를 컬럼으로 변환
        df = df.reset_index()

        # ---------------------------------------------------------
        # [NEW] 날짜 포맷 정제 (YYYY-MM-DD 통일)
        # ---------------------------------------------------------
        # (1) 컬럼명 찾기 ('Date' 또는 'date')
        date_col = 'Date' if 'Date' in df.columns else 'date'
        
        # (2) 섞여있는 날짜 포맷을 표준 datetime 객체로 변환
        df[date_col] = pd.to_datetime(df[date_col])

        # (3) YYYY-MM-DD 문자열 포맷으로 강제 통일 (사용자 선호 반영)
        df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')
        
        # (4) 날짜 기준으로 중복 제거 (가장 마지막 값만 남김)
        df = df.drop_duplicates(subset=[date_col], keep='last')

        # (5) 다시 날짜를 인덱스로 설정
        df = df.set_index(date_col)
        # ---------------------------------------------------------

        # 3. 컬럼 이름 평탄화 (Price, Ticker) -> Price_Ticker
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [f'{col[0]}_{col[1]}' for col in df.columns]
        else:
            df.columns = [f'{col}_{ticker}' for col in df.columns]

        return df.dropna()

    except Exception as e:
        print(f"❌ {ticker} 데이터 수집 실패: {e}")
        return pd.DataFrame()

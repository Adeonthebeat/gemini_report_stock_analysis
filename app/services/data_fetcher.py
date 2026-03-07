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
        # [수정 1] 벤치마크 데이터 가져오기
        market_df = yf.download(benchmark, period="5d", progress=False, auto_adjust=True)
        if market_df.empty:
            return False
        
        # [수정 2] 시장의 최신 날짜를 'YYYY-MM-DD' 포맷으로 추출 (DB와 포맷 통일)
        latest_market_date = market_df.index[-1].strftime('%Y-%m-%d')
        print(f"🔎 시장 최신 데이터 날짜: {latest_market_date}")

    except Exception as e:
        logger.error(f"시장 데이터 확인 중 오류: {e}")
        return False

    with engine.connect() as conn:
        # DB에서 가장 최근 날짜 가져오기
        query = text("select max(date) from price_daily where ticker = :ticker")
        result = conn.execute(query, {"ticker": benchmark}).scalar()

    # [수정 3] DB 날짜가 있다면 문자열로 변환해서 비교
    if result:
        # result가 datetime.date 객체일 경우 문자열로 변환
        db_date_str = str(result)  # '2026-02-02' 형태가 됨
        
        print(f"🗄️ DB 저장된 최신 날짜: {db_date_str}")

        # 문자열끼리 비교 (YYYY-MM-DD >= YYYY-MM-DD)
        if db_date_str >= latest_market_date:
            logger.info(f"✅ 이미 최신 데이터({db_date_str})입니다. 업데이트를 건너뜁니다.")
            return True # 업데이트 안 함

    logger.info(f"🚀 업데이트 필요 (DB: {result} vs Market: {latest_market_date})")
    return False # 업데이트 진행


def fetch_combined_data(ticker, market_type='STOCK', benchmark='VTI'):
    """
    [이전 버전 복구]
    - 리스트를 이용한 일괄 다운로드 방식
    - yfinance의 기본 MultiIndex 구조 활용
    """
    end_date = datetime.now() + timedelta(days=1)
    start_date = end_date - timedelta(days=730)

    print(f"📥 {ticker} 데이터 수집 중... (이전 버전 방식)")

    try:
        # 티커와 벤치마크를 리스트로 묶어 한 번에 다운로드
        df = yf.download([ticker, benchmark], start=start_date, end=end_date,
                         interval='1d', auto_adjust=True, progress=False, group_by='ticker')

        if df.empty:
            return pd.DataFrame()

        # 타임존 제거
        try:
            df.index = df.index.tz_localize(None)
        except:
            pass

        df.index.name = 'Date'

        # MultiIndex 평탄화 (이전 방식)
        if isinstance(df.columns, pd.MultiIndex):
            new_columns = []
            for col in df.columns:
                c1, c2 = str(col[0]), str(col[1])
                # Close_AAPL 또는 Close_VTI 형태로 통일
                if c1 == ticker or c1 == benchmark:
                    new_columns.append(f"{c2}_{c1}")
                else:
                    new_columns.append(f"{c1}_{c2}")
            df.columns = new_columns
        else:
            df.columns = [f"{col}_{ticker}" if ticker not in str(col) else str(col) for col in df.columns]

        # 날짜 포맷 정리
        df = df.reset_index()
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df = df.drop_duplicates(subset=['Date'], keep='last')
            df = df.set_index('Date')
            return df.dropna()

        return pd.DataFrame()

    except Exception as e:
        print(f"❌ {ticker} 수집 중 오류: {e}")
        return pd.DataFrame()
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

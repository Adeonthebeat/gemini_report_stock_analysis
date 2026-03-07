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


def fetch_combined_data(ticker, market_type='STOCK'):
    # 내일 날짜까지 범위를 잡아야 오늘 데이터(미국 현지시간)가 포함됨
    end_date = datetime.now() + timedelta(days=1)
    # 넉넉히 2년치 데이터 요청
    start_date = end_date - timedelta(days=730)

    print(f"📥 {ticker} ({market_type}) 데이터 수집 중... (~{end_date.strftime('%Y-%m-%d')})")

    try:
        # 1. 데이터 다운로드 (에러 무시 옵션 추가)
        df = yf.download(ticker, start=start_date, end=end_date,
                         interval='1d', auto_adjust=True, progress=False)

        # 2. 데이터가 비었는지 확인
        if df.empty:
            print(f"⚠️ {ticker}: 다운로드된 데이터가 없습니다.")
            return pd.DataFrame()

        # ---------------------------------------------------------
        # [Step 1] 인덱스(날짜) 정리부터 먼저 수행 (Timezone 제거)
        # ---------------------------------------------------------
        # yfinance는 종종 타임존이 포함된 날짜를 줍니다. 이걸 먼저 제거해야 뒤탈이 없습니다.
        try:
            df.index = df.index.tz_localize(None)
        except Exception:
            pass # 이미 타임존이 없으면 패스

        df.index.name = 'Date' # 인덱스 이름을 'Date'로 강제 고정

        # 단일 티커를 받았으므로 MultiIndex 평탄화 등 복잡한 로직이 전혀 필요 없습니다!
        # 단순히 기존 코드 포맷(Close_AAPL 형태)에 맞게 컬럼명만 일괄 변경해 줍니다.
        df.columns = [f"{col}_{ticker}" for col in df.columns]

        # 데이터 검증
        target_col = f"Close_{ticker}"
        if target_col not in df.columns:
            print(f"❌ {ticker}: 핵심 데이터({target_col})를 찾을 수 없습니다.")
            return pd.DataFrame()

        # 날짜 포맷 통일 (YYYY-MM-DD)
        df = df.reset_index()

        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df = df.drop_duplicates(subset=['Date'], keep='last')
            df = df.set_index('Date')
            return df.dropna()
        else:
            return pd.DataFrame()

    except Exception as e:
        print(f"❌ {ticker} 처리 중 치명적 오류: {e}")
        # 디버깅을 위해 에러 내용 상세 출력
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

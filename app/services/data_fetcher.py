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
    # 내일 날짜까지 범위를 잡아야 오늘 데이터(미국 현지시간)가 포함됨
    end_date = datetime.now() + timedelta(days=1)
    # 넉넉히 2년치 데이터 요청
    start_date = end_date - timedelta(days=730)

    print(f"📥 {ticker} ({market_type}) vs {benchmark} 데이터 수집 중... (~{end_date.strftime('%Y-%m-%d')})")

    try:
        # 1. 데이터 다운로드 (에러 무시 옵션 추가)
        df = yf.download([ticker, benchmark], start=start_date, end=end_date,
                         interval='1d', auto_adjust=True, progress=False, group_by='ticker')
        
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

        # ---------------------------------------------------------
        # [Step 2] 컬럼 구조 확인 및 평탄화 (여기가 핵심!)
        # ---------------------------------------------------------
        # yfinance가 데이터를 어떻게 줬는지 확인하고 그에 맞춰 처리합니다.
        
        # Case A: MultiIndex인 경우 (일반적인 경우) -> ('AAPL', 'Close') 형태
        if isinstance(df.columns, pd.MultiIndex):
            # yfinance 0.2.x 버전의 group_by='ticker' 옵션은 (Ticker, Price) 순서일 수 있음
            # 혹은 기본 설정은 (Price, Ticker) 순서임.
            # 가장 안전한 방법: 레벨을 확인해서 'Close'가 어디 있는지 찾기보다,
            # 그냥 단순하게 모든 컬럼을 순회하며 이름 짓기
            
            new_columns = []
            for col in df.columns:
                # col은 ('AAPL', 'Close') 또는 ('Close', 'AAPL') 튜플 형태
                # 튜플의 요소들을 언더바(_)로 이어붙임 (순서 상관없이 고유한 이름 생성)
                # 예: AAPL_Close 또는 Close_AAPL
                c1 = str(col[0])
                c2 = str(col[1])
                
                # 우리가 원하는 포맷: 'Close_AAPL' 형태로 통일
                if c1 == ticker or c1 == benchmark: # (Ticker, Price) 순서인 경우
                    new_columns.append(f"{c2}_{c1}")
                else: # (Price, Ticker) 순서인 경우
                    new_columns.append(f"{c1}_{c2}")
            
            df.columns = new_columns

        # Case B: SingleIndex인 경우 (혹시 모를 예외)
        else:
            # 그냥 컬럼 이름 뒤에 티커가 없으면 붙여줌
            df.columns = [f"{col}_{ticker}" if ticker not in col else col for col in df.columns]

        # ---------------------------------------------------------
        # [Step 3] "주인공" 데이터 검증
        # ---------------------------------------------------------
        # 우리가 필요한 'Close_티커' 컬럼이 진짜 있는지 확인
        target_col = f"Close_{ticker}"
        if target_col not in df.columns:
            # 혹시 대소문자 문제일 수도 있으니 확인
            found = False
            for c in df.columns:
                if f"close_{ticker}".lower() == c.lower():
                    df = df.rename(columns={c: target_col})
                    found = True
                    break
            
            if not found:
                print(f"❌ {ticker}: 핵심 데이터({target_col})를 찾을 수 없습니다. 컬럼 목록: {list(df.columns[:5])}...")
                return pd.DataFrame()

        # ---------------------------------------------------------
        # [Step 4] 날짜 포맷 통일 (YYYY-MM-DD)
        # ---------------------------------------------------------
        # 이제 인덱스를 컬럼으로 끄집어냅니다.
        df = df.reset_index()

        # 'Date' 컬럼 포맷팅
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            
            # 날짜 기준 중복 제거 및 인덱스 재설정
            df = df.drop_duplicates(subset=['Date'], keep='last')
            df = df.set_index('Date')
            
            return df.dropna()
        else:
            print(f"❌ {ticker}: 날짜 컬럼 생성 실패")
            return pd.DataFrame()

    except Exception as e:
        print(f"❌ {ticker} 처리 중 치명적 오류: {e}")
        # 디버깅을 위해 에러 내용 상세 출력
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

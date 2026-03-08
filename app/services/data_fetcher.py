import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time  # 💡 [필수 수정] datetime의 time이 아니라, 파이썬 내장 time 모듈을 임포트해야 합니다!
from prefect import task, get_run_logger
from sqlalchemy import text
from app.core.database import get_engine

# 🚀 [추가] 스레드 통신 충돌 방지용 라이브러리
import requests
import threading

# 💡 스레드별로 독립적인 세션을 보장하는 객체
thread_local = threading.local()


@task(name="Check-Market-Update")
def check_market_data_update(benchmark='VTI'):
    logger = get_run_logger()
    engine = get_engine()

    try:
        market_df = yf.download(benchmark, period="5d", progress=False, auto_adjust=True)
        if market_df.empty:
            return False

        latest_market_date = market_df.index[-1].strftime('%Y-%m-%d')
        print(f"🔎 시장 최신 데이터 날짜: {latest_market_date}")

    except Exception as e:
        logger.error(f"시장 데이터 확인 중 오류: {e}")
        return False

    with engine.connect() as conn:
        query = text("select max(date) from price_daily where ticker = :ticker")
        result = conn.execute(query, {"ticker": benchmark}).scalar()

    if result:
        db_date_str = str(result)
        print(f"🗄️ DB 저장된 최신 날짜: {db_date_str}")

        if db_date_str >= latest_market_date:
            logger.info(f"✅ 이미 최신 데이터({db_date_str})입니다. 업데이트를 건너뜁니다.")
            return True

    logger.info(f"🚀 업데이트 필요 (DB: {result} vs Market: {latest_market_date})")
    return False


def fetch_benchmark_data(benchmark='VTI'):
    """💡 [NEW] 벤치마크 데이터를 단 1회 다운로드하여 메모리에 캐싱"""
    end_date = datetime.now() + timedelta(days=1)
    start_date = end_date - timedelta(days=730)

    print(f"🌐 벤치마크({benchmark}) 사전 로드 중...")
    df = yf.download(benchmark, start=start_date, end=end_date, interval='1d', auto_adjust=True, progress=False)

    if df.empty:
        raise ValueError(f"벤치마크 {benchmark} 데이터를 가져오지 못했습니다.")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.index = df.index.tz_localize(None)
    df.index.name = 'Date'
    df.columns = [f"{col}_{benchmark}" for col in df.columns]

    df = df.reset_index()
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    return df.drop_duplicates(subset=['Date'], keep='last').set_index('Date')


def fetch_combined_data(ticker, benchmark_df, market_type='STOCK'):
    """💡 개별 종목 단독 다운로드 및 Pandas 결함 완벽 방어 + 재시도 로직"""
    end_date = datetime.now() + timedelta(days=1)
    start_date = end_date - timedelta(days=730)

    try:  # <--- 문제의 제일 바깥쪽 try 시작
        df = pd.DataFrame()
        for attempt in range(3):
            try:
                # 🚀 [핵심 수정] yf.download() 대신 Ticker 객체 사용! (스레드 충돌 원천 차단)
                ticker_obj = yf.Ticker(ticker)
                df = ticker_obj.history(
                    start=start_date,
                    end=end_date,
                    interval='1d',
                    auto_adjust=True
                )

                if not df.empty:
                    break

            except RuntimeError as e:
                if "dictionary changed size" in str(e):
                    time.sleep(0.5)
                    continue
                else:
                    raise e
            except Exception as e:
                time.sleep(0.5)
                continue

        # 3번 다 실패했거나 진짜로 데이터가 없는 경우
        if df.empty:
            return pd.DataFrame()

        # history() 함수는 컬럼이 한 줄이라 평탄화 작업이 필요 없지만 중복 방지는 둡니다.
        df = df.loc[:, ~df.columns.duplicated()]

        # 유효성 검사
        if 'Close' not in df.columns or bool(df['Close'].isna().all()):
            print(f"⚠️ {ticker}: 유효한 가격 데이터 없음 (상폐 의심)")
            return pd.DataFrame()

        # 인덱스 및 컬럼명 정리
        df.index = df.index.tz_localize(None)
        df.index.name = 'Date'
        df.columns = [f"{col}_{ticker}" for col in df.columns]

        df = df.reset_index()
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        df = df.drop_duplicates(subset=['Date'], keep='last').set_index('Date')

        # VTI 조인 처리
        if ticker == 'VTI':
            combined_df = df
        else:
            combined_df = df.join(benchmark_df, how='left')

        target_col = f"Close_{ticker}"
        return combined_df.dropna(subset=[target_col])

    # 🚀 [복구 완료!] 바깥쪽 try와 짝을 이루는 except 구문들
    except ValueError as e:
        if "No objects to concatenate" in str(e):
            print(f"⚠️ {ticker}: 데이터 없음 (상장폐지/티커변경 의심 - 제외 처리됨)")
        else:
            print(f"❌ {ticker} 값 오류: {e}")
        return pd.DataFrame()

    except Exception as e:
        print(f"❌ {ticker} 처리 중 알 수 없는 에러: {e}")
        return pd.DataFrame()

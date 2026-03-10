import random as rnd

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
    end_date = datetime.now() + timedelta(days=1)
    start_date = end_date - timedelta(days=730)

    try:
        df = pd.DataFrame()
        try:

            # 🚀 [핵심 수정] Ticker.history 대신 yf.download 사용!
            # 다중 스레드 환경에서 세션 충돌과 IP 차단에 훨씬 강합니다.
            df = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                progress=False,
                auto_adjust=True,
                threads=False  # yf 내부 스레딩 끄기 (이미 밖에서 스레드를 쓰고 있으므로)
            )

        except Exception as e:
            print(f"🔄 [{ticker}] 다운로드 에러({e})")

        if df.empty:
            return pd.DataFrame()

        # 🚀 yf.download 단일 종목 호출 시 발생하는 멀티인덱스 컬럼 평탄화 작업
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # 유효성 검사
        if 'Close' not in df.columns or bool(df['Close'].isna().all()):
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

    except Exception as e:
        print(f"❌ {ticker} 처리 중 알 수 없는 에러: {e}")
        return pd.DataFrame()
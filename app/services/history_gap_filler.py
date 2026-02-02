import yfinance as yf
import pandas as pd
from sqlalchemy import text
from app.core.database import get_engine
from prefect import flow, task, get_run_logger
import time


@task(name="Gap-Fill-Price-Data")
def gap_fill_stock_prices(period="1y"):
    """
    [Gap Fill 모드]
    price_daily 테이블에 데이터가 '없는' 종목만 골라서 과거 데이터를 수집합니다.
    """
    logger = get_run_logger()
    engine = get_engine()

    with engine.connect() as conn:
        # 1. 전체 종목 리스트 (Master)
        master_query = text("SELECT ticker FROM stock_master")
        all_tickers = {row.ticker for row in conn.execute(master_query).fetchall()}  # 집합(Set)으로 변환

        # 2. 이미 수집된 종목 리스트 (Price Daily)
        # DISTINCT를 사용하여 티커 목록만 빠르게 가져옵니다.
        exists_query = text("SELECT DISTINCT ticker FROM price_daily")
        existing_tickers = {row.ticker for row in conn.execute(exists_query).fetchall()}

    # 3. 차집합 연산 (전체 - 이미 있는 것 = 해야 할 것)
    target_tickers = list(all_tickers - existing_tickers)

    if not target_tickers:
        logger.info("✨ 모든 종목의 데이터가 이미 존재합니다. 작업을 종료합니다.")
        return

    logger.info(f"🧩 누락 데이터 채우기 시작: 총 {len(target_tickers)}개 종목 대상 (기간: {period})")

    success_count = 0

    for ticker in target_tickers:
        try:
            logger.info(f"   ▶ 수집 시도: {ticker}")

            # yfinance로 데이터 다운로드
            df = yf.download(ticker, period=period, progress=False, auto_adjust=True)

            if df.empty:
                logger.warning(f"   ⚠️ {ticker}: 데이터 없음 (티커 확인 필요)")
                continue

            # 데이터 전처리
            df = df.reset_index()
            df['Date'] = pd.to_datetime(df['Date']).dt.date

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            rows_to_insert = []
            for _, row in df.iterrows():
                data = {
                    "ticker": ticker,
                    "date": row['Date'],
                    "open": float(row['Open']),
                    "high": float(row['High']),
                    "low": float(row['Low']),
                    "close": float(row['Close']),
                    "volume": int(row['Volume'])
                }
                rows_to_insert.append(data)

            # DB 저장
            if rows_to_insert:
                with engine.begin() as conn:
                    stmt = text("""
                        INSERT INTO price_daily (ticker, date, open, high, low, close, volume)
                        VALUES (:ticker, :date, :open, :high, :low, :close, :volume)
                        ON CONFLICT (ticker, date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume
                    """)
                    conn.execute(stmt, rows_to_insert)

                success_count += 1
                logger.info(f"   ✅ {ticker}: 저장 완료")

            time.sleep(0.5)

        except Exception as e:
            logger.error(f"❌ {ticker} 처리 중 오류: {e}")

    logger.info(f"🎉 누락분 채우기 완료! 총 {success_count}개 종목 처리됨.")


if __name__ == "__main__":
    @flow(name="Manual-Gap-Fill")
    def run_gap_fill():
        gap_fill_stock_prices(period="1y")  # 혹은 'max'


    run_gap_fill()
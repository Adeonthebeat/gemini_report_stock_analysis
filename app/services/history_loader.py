import yfinance as yf
import pandas as pd
from sqlalchemy import text
from app.core.database import get_engine
from prefect import flow, task, get_run_logger
import time


@task(name="Backfill-Price-Data")
def backfill_stock_prices(period="1y"):
    """
    모든 등록된 주식의 과거 데이터를 한꺼번에 수집하여 DB에 적재
    :param period: 수집 기간 (예: '1y', '2y', '5y', 'max') -> 200일선 분석을 위해 기본 1년 권장
    """
    logger = get_run_logger()
    engine = get_engine()

    # 1. 수집 대상 종목 가져오기
    with engine.connect() as conn:
        # ETF, STOCK 가리지 않고 다 가져옵니다.
        query = text("SELECT ticker FROM stock_master")
        tickers = [row.ticker for row in conn.execute(query).fetchall()]

    logger.info(f"📚 과거 데이터 수집 시작: 총 {len(tickers)}개 종목 (기간: {period})")

    total_count = 0

    for ticker in tickers:
        try:
            # 2. yfinance로 데이터 다운로드
            # auto_adjust=True: 액면분할/배당락 수정 주가 사용
            df = yf.download(ticker, period=period, progress=False, auto_adjust=True)

            if df.empty:
                logger.warning(f"⚠️ {ticker}: 데이터 없음")
                continue

            # 3. 데이터 전처리
            df = df.reset_index()  # Date를 컬럼으로 뺌
            df['Date'] = pd.to_datetime(df['Date']).dt.date  # 시간 제거하고 날짜만 남김

            # DB 컬럼명에 맞게 변경 (Date -> date, Close -> close 등)
            # yfinance 최신 버전은 컬럼이 MultiIndex일 수 있음. 단순화 처리.
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

            # 4. DB에 저장 (Upsert: 중복되면 업데이트)
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

                total_count += len(rows_to_insert)
                logger.info(f"   ✅ {ticker}: {len(rows_to_insert)}일치 데이터 저장 완료")

            # 너무 빨리 요청하면 차단될 수 있으니 살짝 쉬기
            time.sleep(0.5)

        except Exception as e:
            logger.error(f"❌ {ticker} 수집 실패: {e}")

    logger.info(f"🎉 전체 초기화 완료! 총 {total_count}개의 일봉 데이터가 쌓였습니다.")


# 단독 실행을 위한 코드
if __name__ == "__main__":
    @flow(name="Manual-History-Load")
    def run_backfill():
        backfill_stock_prices(period="1y")  # 1년치 데이터 (200일선 분석 가능)


    run_backfill()

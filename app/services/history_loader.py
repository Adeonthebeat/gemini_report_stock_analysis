import yfinance as yf
import pandas as pd
from sqlalchemy import text
from app.core.database import get_engine
from prefect import flow, task, get_run_logger
import time


@task(name="Backfill-Price-Data")
def backfill_stock_prices(period="2y"):
    """
    price_daily 테이블에 데이터가 없는 신규 종목만 골라 과거 데이터를 수집합니다.
    :param period: 기본 2년치 수집
    """
    logger = get_run_logger()
    engine = get_engine()

    # ---------------------------------------------------------
    # [수정 1] 수집 대상 필터링 (전체 - 이미 있는 것)
    # ---------------------------------------------------------
    with engine.connect() as conn:
        # 1. 전체 종목 리스트 (Master)
        master_query = text("SELECT ticker FROM stock_master")
        master_result = conn.execute(master_query).fetchall()
        master_tickers = {row[0] for row in master_result}  # 집합(Set)으로 변환

        # 2. 이미 데이터가 있는 종목 리스트 (Existing)
        # DISTINCT를 사용하여 중복 없이 티커만 가져옵니다.
        exist_query = text("SELECT DISTINCT ticker FROM price_daily where date < '2026-02-02'")
        exist_result = conn.execute(exist_query).fetchall()
        exist_tickers = {row[0] for row in exist_result}

    # 3. 차집합 연산: 전체 - 이미 있는 것 = 해야 할 것
    target_tickers = list(master_tickers - exist_tickers)

    if not target_tickers:
        logger.info("✅ 모든 종목의 데이터가 이미 존재합니다. 작업을 종료합니다.")
        return

    logger.info(f"📚 데이터 수집 시작")
    logger.info(f"   - 전체 등록 종목: {len(master_tickers)}개")
    logger.info(f"   - 이미 데이터 있음: {len(exist_tickers)}개")
    logger.info(f"   - 🚀 수집 대상(신규): {len(target_tickers)}개 (기간: {period})")

    total_count = 0
    success_ticker_count = 0

    # target_tickers로 루프 시작
    for i, ticker in enumerate(target_tickers):
        try:
            # 진행 상황 표시 (10개마다 로그)
            if i % 10 == 0:
                logger.info(f"🚀 진행중... ({i + 1}/{len(target_tickers)}) 현재: {ticker}")

            # ---------------------------------------------------------
            # 2. yfinance로 데이터 다운로드 (이하 동일)
            # ---------------------------------------------------------
            df = yf.download(ticker, period=period, progress=False, auto_adjust=True)

            if df.empty:
                logger.warning(f"⚠️ {ticker}: 데이터 없음 (상장폐지 또는 티커 변경 가능성)")
                continue

            # (1) MultiIndex 컬럼 평탄화
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # (2) 인덱스(Date)를 컬럼으로 변환
            df = df.reset_index()

            # (3) 날짜 컬럼 찾기
            date_col = 'Date' if 'Date' in df.columns else 'date'
            if date_col not in df.columns:
                logger.error(f"❌ {ticker}: 날짜 컬럼 없음")
                continue

            # (4) 날짜 포맷 통일
            df['date_str'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')

            # ---------------------------------------------------------
            # 3. DB 저장용 데이터 생성
            # ---------------------------------------------------------
            rows_to_insert = []
            for _, row in df.iterrows():
                try:
                    data = {
                        "ticker": ticker,
                        "date": row['date_str'],
                        "open": float(row.get('Open', 0)),
                        "high": float(row.get('High', 0)),
                        "low": float(row.get('Low', 0)),
                        "close": float(row.get('Close', 0)),
                        "volume": int(row.get('Volume', 0))
                    }
                    rows_to_insert.append(data)
                except Exception:
                    continue

            # 4. DB에 저장
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
                success_ticker_count += 1

            # 서버 부하 방지용 대기
            time.sleep(0.2)

        except Exception as e:
            logger.error(f"❌ {ticker} 수집 중 오류: {e}")

    logger.info(f"🎉 신규 종목 백필 완료!")
    logger.info(f"   - 성공 종목: {success_ticker_count} / {len(target_tickers)}")
    logger.info(f"   - 총 추가된 행: {total_count}개")


# 단독 실행
if __name__ == "__main__":
    @flow(name="Manual-History-Load")
    def run_backfill():
        backfill_stock_prices(period="2y")


    run_backfill()
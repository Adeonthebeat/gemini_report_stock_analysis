import yfinance as yf
import pandas as pd
from sqlalchemy import text
from app.core.database import get_engine
from prefect import flow, task, get_run_logger
import time

@task(name="Backfill-Price-Data")
def backfill_stock_prices(period="2y"):
    """
    모든 등록된 주식의 과거 데이터를 한꺼번에 수집하여 DB에 적재
    :param period: 200일선 + RSI 등을 여유롭게 계산하기 위해 '2y'(2년) 추천
    """
    logger = get_run_logger()
    engine = get_engine()

    # 1. 수집 대상 종목 가져오기
    with engine.connect() as conn:
        query = text("SELECT ticker FROM stock_master")
        result = conn.execute(query).fetchall()
        tickers = [row[0] for row in result] # row.ticker 대신 row[0]이 더 안전할 수 있음

    logger.info(f"📚 과거 데이터 수집 시작: 총 {len(tickers)}개 종목 (기간: {period})")

    total_count = 0
    success_ticker_count = 0

    for i, ticker in enumerate(tickers):
        try:
            # 진행 상황 표시 (10개마다 로그)
            if i % 10 == 0:
                logger.info(f"🚀 진행중... ({i}/{len(tickers)}) 현재: {ticker}")

            # 2. yfinance로 데이터 다운로드
            # auto_adjust=True: 수정주가(액면분할/배당 반영)
            df = yf.download(ticker, period=period, progress=False, auto_adjust=True)

            if df.empty:
                logger.warning(f"⚠️ {ticker}: 데이터 없음 (상장폐지 또는 티커 변경 가능성)")
                continue

            # ---------------------------------------------------------
            # [핵심 수정] 데이터 전처리 (yfinance 버전 호환성 강화)
            # ---------------------------------------------------------
            
            # (1) MultiIndex 컬럼 평탄화 ('Close', 'AAPL') -> 'Close'
            if isinstance(df.columns, pd.MultiIndex):
                # 레벨 0(Price)만 남기고 티커 이름 제거
                df.columns = df.columns.get_level_values(0)

            # (2) 인덱스(Date)를 컬럼으로 변환
            df = df.reset_index()

            # (3) 날짜 컬럼 찾기 ('Date' or 'date')
            date_col = 'Date' if 'Date' in df.columns else 'date'
            if date_col not in df.columns:
                logger.error(f"❌ {ticker}: 날짜 컬럼을 찾을 수 없음. 컬럼: {df.columns}")
                continue

            # (4) 날짜 포맷 통일 (Timezone 제거 -> YYYY-MM-DD 문자열)
            # DB 저장 시 문자열로 주면 Postgres가 알아서 DATE 타입으로 받아줌
            df['date_str'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')

            # ---------------------------------------------------------
            # 3. DB 저장용 데이터 생성
            # ---------------------------------------------------------
            rows_to_insert = []
            for _, row in df.iterrows():
                # 필수 컬럼 값 가져오기 (없으면 0 처리)
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
                except Exception as inner_e:
                    logger.warning(f"⚠️ {ticker} 행 변환 중 오류: {inner_e}")
                    continue

            # 4. DB에 저장 (Batch Insert)
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
                # logger.info(f"   ✅ {ticker}: {len(rows_to_insert)}건 저장")

            # 서버 부하 방지용 짧은 대기
            time.sleep(0.2)

        except Exception as e:
            logger.error(f"❌ {ticker} 수집 중 치명적 오류: {e}")

    logger.info(f"🎉 전체 초기화 완료!")
    logger.info(f"   - 성공 종목: {success_ticker_count} / {len(tickers)}")
    logger.info(f"   - 총 데이터 행: {total_count}개")

# 단독 실행을 위한 코드
if __name__ == "__main__":
    @flow(name="Manual-History-Load")
    def run_backfill():
        backfill_stock_prices(period="2y") # 넉넉하게 2년치

    run_backfill()

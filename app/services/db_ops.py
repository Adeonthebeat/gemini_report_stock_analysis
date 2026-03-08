import math

import pandas as pd
from prefect import task, flow, get_run_logger
from sqlalchemy import text
from app.core.database import get_engine


@task(name="Get-Tickers")
def get_tickers():
    """
    분석 대상 티커와 마켓 타입(STOCK/SECTOR)을 가져옵니다.
    """
    engine = get_engine()
    with engine.connect() as conn:
        # [수정] 테이블명 소문자(stock_master)로 변경
        # market_type 컬럼이 없어도 에러나지 않게 처리하려면 스키마 확인이 필요하지만,
        # 앞서 마이그레이션을 했다고 가정하고 SELECT 합니다.
        try:
            query = text("SELECT ticker, market_type FROM stock_master where 1=1 ")
            result = conn.execute(query).fetchall()
            return [{'ticker': row.ticker, 'market_type': row.market_type} for row in result]
        except Exception:
            # 혹시 market_type 컬럼이 아직 없다면 기본값 처리
            query = text("SELECT ticker FROM stock_master")
            result = conn.execute(query).fetchall()
            return [{'ticker': row.ticker, 'market_type': 'STOCK'} for row in result]


@task(name="Save-Data-Bulk")
def save_to_sqlite_bulk(daily_list, weekly_list, chunk_size=500):
    logger = get_run_logger()
    engine = get_engine()

    # 1. 치명적 원인 차단: NaN 변환 + 누락된 키 자동 보완
    def clean_data(data_list, table_type):
        cleaned = []
        for row in data_list:
            clean_row = {}
            # NaN을 None으로 변환
            for k, v in row.items():
                if isinstance(v, float) and math.isnan(v):
                    clean_row[k] = None
                else:
                    clean_row[k] = v

            # [핵심 수정] Weekly 데이터의 경우 쿼리에서 요구하는 키가 없으면 None으로 채움
            if table_type == "weekly":
                if 'is_vcp' not in clean_row: clean_row['is_vcp'] = None
                if 'is_vol_dry' not in clean_row: clean_row['is_vol_dry'] = None
                if 'atr_stop_loss' not in clean_row: clean_row['atr_stop_loss'] = None

            cleaned.append(clean_row)
        return cleaned

    # 데이터 클렌징 실행
    cleaned_daily = clean_data(daily_list, "daily")
    cleaned_weekly = clean_data(weekly_list, "weekly")

    def execute_in_chunks(table_name, data_list, query):
        total = len(data_list)
        if total == 0: return

        for i in range(0, total, chunk_size):
            chunk = data_list[i: i + chunk_size]

            try:
                # 🚀 [핵심] 트랜잭션(begin)을 for문 안으로 이동!
                # 50개가 끝날 때마다 무조건 DB에 영구 저장(Commit) 때려버림
                with engine.begin() as conn:
                    conn.execute(text(query), chunk)

                logger.info(f"   └ 📦 [{table_name}] {min(i + chunk_size, total)}/{total} 영구 저장 완료!")

            except Exception as e:
                # 에러가 나면 해당 50개만 실패하고, 앞서 넣은 데이터는 안전하게 보존됨
                logger.error(f"❌ [{table_name}] {i}번째 구간 에러: {e}")
    # 2. 명시적 트랜잭션 에러 캡처 적용
    try:
        if cleaned_daily:
            daily_query = """
                        INSERT INTO price_daily (ticker, date, open, high, low, close, volume)
                        VALUES (:ticker, :date, :open, :high, :low, :close, :volume)
                        ON CONFLICT(ticker, date) 
                        DO UPDATE SET 
                            open = EXCLUDED.open,    -- 🚀 [핵심 수정] 누락되어 있던 open 값 업데이트 추가
                            close = EXCLUDED.close, 
                            volume = EXCLUDED.volume,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low
                    """
            execute_in_chunks("price_daily", cleaned_daily, daily_query)

        if cleaned_weekly:
            weekly_query = """
                INSERT INTO price_weekly (
                    ticker, weekly_date, weekly_return, rs_value, 
                    is_above_200ma, deviation_200ma, is_vcp, is_vol_dry, atr_stop_loss
                )
                VALUES (
                    :ticker, :weekly_date, :weekly_return, :rs_value, 
                    :is_above_200ma, :deviation_200ma, :is_vcp, :is_vol_dry, :atr_stop_loss
                )
                ON CONFLICT(ticker, weekly_date) 
                DO UPDATE SET 
                    rs_value = EXCLUDED.rs_value, 
                    is_above_200ma = EXCLUDED.is_above_200ma,
                    deviation_200ma = EXCLUDED.deviation_200ma,
                    is_vcp = EXCLUDED.is_vcp,
                    is_vol_dry = EXCLUDED.is_vol_dry,
                    atr_stop_loss = EXCLUDED.atr_stop_loss
            """
            execute_in_chunks("price_weekly", cleaned_weekly, weekly_query)

        logger.info(f"✅ 총 {len(cleaned_daily)}개 일간 / {len(cleaned_weekly)}개 주간 데이터 벌크 저장 최종 완료!")

    except Exception as e:
        logger.error(f"❌ DB 벌크 저장 중 치명적 에러 발생: {e}")


def get_finished_tickers(target_date_str):
    """
    이미 작업이 완료된(DB에 해당 날짜 데이터가 있는) 티커 목록을 가져옵니다.
    Set 자료형으로 반환하여 검색 속도를 높입니다.
    """
    engine = get_engine()

    query = text("SELECT ticker FROM price_daily WHERE date = :date")

    with engine.connect() as conn:
        result = conn.execute(query, {"date": target_date_str}).fetchall()

    # 예: {'AAPL', 'TSLA', 'NVDA'} 형태의 집합(Set)으로 반환
    return {row[0] for row in result}


def check_db_insertion():
    engine = get_engine()

    with engine.connect() as conn:
        print("📊 [price_daily] 최신 입력 데이터 3건:")
        daily_query = text("SELECT * FROM price_daily ORDER BY date DESC LIMIT 3")
        print(pd.read_sql(daily_query, conn))

        print("\n📊 [price_weekly] 최신 입력 데이터 3건:")
        weekly_query = text("SELECT * FROM price_weekly ORDER BY weekly_date DESC LIMIT 3")
        print(pd.read_sql(weekly_query, conn))

        # 전체 데이터 개수 카운트
        daily_cnt = conn.execute(text("SELECT count(*) FROM price_daily")).scalar()
        weekly_cnt = conn.execute(text("SELECT count(*) FROM price_weekly")).scalar()
        print(f"\n✅ 현재 DB 총 데이터 수 - 일간: {daily_cnt}개 / 주간: {weekly_cnt}개")


if __name__ == "__main__":
    check_db_insertion()

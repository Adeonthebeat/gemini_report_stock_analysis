import yfinance as yf
import pandas as pd
from sqlalchemy import text, create_engine
from datetime import datetime, timedelta


# 기존 코드에 있는 get_engine 함수 사용 (또는 직접 import)
# from your_main_script import get_engine
def get_engine():
    return create_engine(f"sqlite:///my_stock_data.db?check_same_thread=False")

def insert_vti_history_manually():
    print("🚀 VTI 데이터 긴급 복구 작업을 시작합니다...")

    ENGINE = get_engine()  # 기존에 정의된 엔진 사용

    # 1. VTI 데이터 다운로드 (넉넉하게 700일치 - RS 계산용 여유분 포함)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    print(f"1. Yahoo Finance에서 다운로드 중... ({start_date.date()} ~ {end_date.date()})")
    df = yf.download('VTI', start=start_date, end=end_date, interval='1d', auto_adjust=False, progress=False)

    if df.empty:
        print("❌ 에러: 데이터를 가져올 수 없습니다.")
        return

    # 2. DB에 대량 삽입 (Bulk Insert)
    print(f"2. DB 입력 시작 (총 {len(df)}건)...")

    count = 0
    with ENGINE.begin() as conn:
        for index, row in df.iterrows():
            # 날짜 포맷 변환 (YYYYMMDD)
            date_str = index.strftime('%Y%m%d')

            # 데이터 매핑
            # yfinance 최신 버전에 따라 컬럼명이 MultiIndex일 수 있어 단순화 처리
            try:
                open_val = float(row['Open'].iloc[0]) if isinstance(row['Open'], pd.Series) else float(row['Open'])
                high_val = float(row['High'].iloc[0]) if isinstance(row['High'], pd.Series) else float(row['High'])
                low_val = float(row['Low'].iloc[0]) if isinstance(row['Low'], pd.Series) else float(row['Low'])
                close_val = float(row['Close'].iloc[0]) if isinstance(row['Close'], pd.Series) else float(row['Close'])
                vol_val = int(row['Volume'].iloc[0]) if isinstance(row['Volume'], pd.Series) else int(row['Volume'])
            except Exception:
                # MultiIndex가 아닌 경우 일반 접근
                open_val = float(row['Open'])
                high_val = float(row['High'])
                low_val = float(row['Low'])
                close_val = float(row['Close'])
                vol_val = int(row['Volume'])

            # INSERT 쿼리 (중복 시 가격 업데이트)
            stmt = text("""
                INSERT INTO PRICE_DAILY (TICKER, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
                VALUES ('VTI', :date, :open, :high, :low, :close, :volume)
                ON CONFLICT(TICKER, DATE) DO UPDATE SET 
                    CLOSE = excluded.CLOSE, 
                    VOLUME = excluded.VOLUME
            """)

            conn.execute(stmt, {
                "date": date_str,
                "open": open_val,
                "high": high_val,
                "low": low_val,
                "close": close_val,
                "volume": vol_val
            })
            count += 1

    print(f"✅ 성공: VTI 데이터 {count}건이 PRICE_DAILY에 저장되었습니다.")


# 실행
if __name__ == "__main__":
    insert_vti_history_manually()
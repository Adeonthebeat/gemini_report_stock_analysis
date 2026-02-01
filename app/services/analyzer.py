from prefect import task, get_run_logger
from sqlalchemy import text
from app.core.database import get_engine
import pandas as pd


@task(name="Calculate-Metrics")
def calculate_metrics(df, ticker, benchmark='VTI'):
    # 데이터 길이 체크
    if df.empty or len(df) < 252:
        # 에러 발생시키기보다 로그 찍고 None 리턴하는 게 파이프라인 안 끊기고 좋음
        print(f"⚠️ {ticker}: 데이터 부족 (1년 미만)")
        return None, None

    # 윌리엄 오닐 스타일 가중 수익률
    def calc_weighted_return(series):
        if len(series) < 252: return 0
        try:
            curr = series.iloc[-1]
            r1 = (curr / series.iloc[-63]) - 1
            r2 = (series.iloc[-63] / series.iloc[-126]) - 1
            r3 = (series.iloc[-126] / series.iloc[-189]) - 1
            r4 = (series.iloc[-189] / series.iloc[-252]) - 1
            return (r1 * 0.4) + (r2 * 0.2) + (r3 * 0.2) + (r4 * 0.2)
        except IndexError:
            return 0

    # 컬럼 이름이 'Close_AAPL', 'Close_VTI' 형식으로 들어옴
    try:
        t_close = df[f'Close_{ticker}']
        b_close = df[f'Close_{benchmark}']
    except KeyError:
        print(f"❌ {ticker}: 컬럼 찾기 실패. (fetch_combined_data 컬럼명 확인 필요)")
        return None, None

    # 지표 계산
    rs_score = (calc_weighted_return(t_close) - calc_weighted_return(b_close)) * 100
    current_price = float(t_close.iloc[-1])
    sma200 = float(t_close.rolling(window=200).mean().iloc[-1])
    weekly_return = ((current_price / t_close.iloc[-6]) - 1) * 100

    # [중요] DB 저장용 딕셔너리 키를 '소문자'로 변경 (Supabase 호환)
    daily_data = {
        "ticker": ticker,
        "date": df.index[-1].strftime('%Y-%m-%d'),  # 포맷 변경 YYYY-MM-DD
        "open": float(df[f'Open_{ticker}'].iloc[-1]),
        "high": float(df[f'High_{ticker}'].iloc[-1]),
        "low": float(df[f'Low_{ticker}'].iloc[-1]),
        "close": current_price,
        "volume": int(df[f'Volume_{ticker}'].iloc[-1])
    }

    weekly_data = {
        "ticker": ticker,
        "weekly_date": df.index[-1].strftime('%Y-%m-%d'),
        "weekly_return": round(float(weekly_return), 2),
        "rs_value": round(float(rs_score), 2),
        "is_above_200ma": 1 if current_price > sma200 else 0,
        "deviation_200ma": round(((current_price / sma200) - 1) * 100, 2)
    }

    return daily_data, weekly_data


@task(name="Update-RS-Indicators")
def update_rs_indicators():
    logger = get_run_logger()
    engine = get_engine()

    with engine.begin() as conn:
        # 1. 컬럼 추가 (소문자 테이블/컬럼명 사용)
        cols = ["rs_rating REAL", "stock_grade VARCHAR(10)", "rs_momentum REAL"]
        for col in cols:
            try:
                conn.execute(text(f"ALTER TABLE price_weekly ADD COLUMN IF NOT EXISTS {col}"))
            except Exception:
                pass

        # 2. RS 랭킹 업데이트 쿼리 (소문자 적용)
        query = text("""
            UPDATE price_weekly
            SET rs_rating = sub.new_rating, 
                rs_momentum = sub.new_momentum,
                stock_grade = CASE 
                    WHEN sub.new_rating >= 90 THEN 'A' 
                    WHEN sub.new_rating >= 70 THEN 'B'
                    WHEN sub.new_rating >= 50 THEN 'C' 
                    WHEN sub.new_rating >= 30 THEN 'D' 
                    ELSE 'E' 
                END
            FROM (
                SELECT ticker, weekly_date,
                    ROUND(CAST(PERCENT_RANK() OVER (PARTITION BY weekly_date ORDER BY rs_value ASC) * 100 AS NUMERIC), 0) as new_rating,
                    rs_value - LAG(rs_value) OVER (PARTITION BY ticker ORDER BY weekly_date ASC) as new_momentum
                FROM price_weekly
            ) AS sub
            WHERE price_weekly.ticker = sub.ticker 
              AND price_weekly.weekly_date = sub.weekly_date;
        """)
        conn.execute(query)

    logger.info("✅ RS 지표(Rating, Grade) 업데이트 완료")
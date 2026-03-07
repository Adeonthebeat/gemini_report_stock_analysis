from prefect import task, get_run_logger
from sqlalchemy import text
from app.core.database import get_engine
import pandas as pd


@task(name="Calculate-Metrics")
def calculate_metrics(df, ticker, benchmark='VTI'):
    # 1. 데이터 길이 체크 (퀀트 분석을 위해 최소 1년치인 252거래일 필요)
    if df.empty or len(df) < 252:
        print(f"⚠️ {ticker}: 데이터 부족 (현재 {len(df)}행, 최소 252행 필요)")
        return None, None

    # 윌리엄 오닐 스타일 가중 수익률 계산 함수
    def calc_weighted_return(series):
        try:
            curr = series.iloc[-1]
            r1 = (curr / series.iloc[-63]) - 1  # 최근 3개월
            r2 = (series.iloc[-63] / series.iloc[-126]) - 1
            r3 = (series.iloc[-126] / series.iloc[-189]) - 1
            r4 = (series.iloc[-189] / series.iloc[-252]) - 1
            return (r1 * 0.4) + (r2 * 0.2) + (r3 * 0.2) + (r4 * 0.2)
        except Exception:
            return 0

    # 2. [핵심] 리스트 다운로드 방식의 컬럼명 참조 (Close_Ticker 형태)
    try:
        t_close = df[f'Close_{ticker}']
        t_high = df[f'High_{ticker}']
        t_low = df[f'Low_{ticker}']
        t_vol = df[f'Volume_{ticker}']
        b_close = df[f'Close_{benchmark}']
    except KeyError as e:
        print(f"❌ {ticker}: 컬럼 매핑 실패 ({e}). 데이터 구조를 확인하세요.")
        return None, None

    # 3. 기본 지표 계산
    # RS Score: 종목의 가중수익률에서 벤치마크의 가중수익률을 뺌
    rs_score = (calc_weighted_return(t_close) - calc_weighted_return(b_close)) * 100

    current_price = float(t_close.iloc[-1])
    sma200 = float(t_close.rolling(window=200).mean().iloc[-1])
    weekly_return = ((current_price / t_close.iloc[-6]) - 1) * 100

    # 4. 💡 VCP (변동성 수축 필터)
    # 60일 평균 변동성 대비 최근 20일 변동성이 75% 이하로 줄었는지 확인
    daily_range = (t_high - t_low) / t_close
    volatility_20d = daily_range.tail(20).mean()
    volatility_60d = daily_range.tail(60).mean()
    is_vcp = 1 if (volatility_60d > 0 and volatility_20d < (volatility_60d * 0.75)) else 0

    # 5. 💡 Volume Dry-up (거래량 고갈 필터)
    # 50일 평균 거래량 대비 최근 5일 평균 거래량이 60% 이하로 감소했는지 확인
    vol_50d_avg = t_vol.tail(50).mean()
    vol_5d_avg = t_vol.tail(5).mean()
    is_vol_dry = 1 if (vol_50d_avg > 0 and vol_5d_avg < (vol_50d_avg * 0.6)) else 0

    # 6. 💡 ATR 기반 동적 리스크 관리
    prev_close = t_close.shift(1)
    tr = pd.concat([t_high - t_low, (t_high - prev_close).abs(), (t_low - prev_close).abs()], axis=1).max(axis=1)
    atr_14 = float(tr.tail(14).mean())
    atr_stop_loss = round(current_price - (2 * atr_14), 2)

    # 7. 날짜 처리
    latest_date_obj = pd.to_datetime(df.index[-1])
    formatted_date = latest_date_obj.strftime('%Y-%m-%d')

    # 8. DB 저장용 데이터 구성
    daily_data = {
        "ticker": ticker,
        "date": formatted_date,
        "open": round(float(df[f'Open_{ticker}'].iloc[-1]), 2),
        "high": round(float(df[f'High_{ticker}'].iloc[-1]), 2),
        "low": round(float(df[f'Low_{ticker}'].iloc[-1]), 2),
        "close": round(current_price, 2),
        "volume": int(df[f'Volume_{ticker}'].iloc[-1])
    }

    weekly_data = {
        "ticker": ticker,
        "weekly_date": formatted_date,
        "weekly_return": round(float(weekly_return), 2),
        "rs_value": round(float(rs_score), 2),
        "is_above_200ma": 1 if current_price > sma200 else 0,
        "deviation_200ma": round(((current_price / sma200) - 1) * 100, 2),
        "is_vcp": is_vcp,
        "is_vol_dry": is_vol_dry,
        "atr_stop_loss": atr_stop_loss
    }

    return daily_data, weekly_data


@task(name="Update-RS-Indicators")
def update_rs_indicators():
    logger = get_run_logger()
    engine = get_engine()

    with engine.begin() as conn:

        # RS 랭킹 업데이트 쿼리 (소문자 적용)
        query = text("""
            WITH rank_calc AS (
                -- 1단계: 날짜별 RS Rating 계산
                SELECT ticker, weekly_date, rs_value,
                       ROUND(CAST(PERCENT_RANK() OVER (PARTITION BY weekly_date ORDER BY rs_value ASC) * 100 AS NUMERIC), 0) as new_rating
                FROM price_weekly
            ),
            trend_calc AS (
                -- 2단계: 티커별 과거 데이터를 바탕으로 모멘텀과 4주 평균 계산
                SELECT ticker, weekly_date, new_rating,
                       new_rating - LAG(new_rating) OVER (PARTITION BY ticker ORDER BY weekly_date ASC) as new_momentum,
                       AVG(new_rating) OVER (PARTITION BY ticker ORDER BY weekly_date ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) as avg_4w
                FROM rank_calc
            )
            -- 3단계: 최종 업데이트 (윈도우 함수 없이 값만 매핑)
            UPDATE price_weekly
            SET rs_rating = t.new_rating, 
                rs_momentum = t.new_momentum,
                rs_trend = CASE WHEN t.new_rating >= t.avg_4w THEN 'UP' ELSE 'DOWN' END,
                stock_grade = CASE 
                    WHEN t.new_rating >= 90 THEN 'A' 
                    WHEN t.new_rating >= 70 THEN 'B'
                    WHEN t.new_rating >= 50 THEN 'C' 
                    WHEN t.new_rating >= 30 THEN 'D' 
                    ELSE 'E' 
                END
            FROM trend_calc t
            WHERE price_weekly.ticker = t.ticker 
              AND price_weekly.weekly_date = t.weekly_date;
        """)
        conn.execute(query)

    logger.info("✅ RS 지표(Rating, Grade, Trend) 업데이트 완료")

from prefect import task, get_run_logger
from sqlalchemy import text
from app.core.database import get_engine
import pandas as pd


@task(name="Calculate-Metrics")
def calculate_metrics(df, ticker, benchmark='VTI'):

    # 데이터 길이 체크
    if df.empty or len(df) < 252:
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
        # [수정] 컬럼 이름 매핑
        t_close = df[f'Close_{ticker}']
        t_high = df[f'High_{ticker}']
        t_low = df[f'Low_{ticker}']
        t_vol = df[f'Volume_{ticker}']
    except KeyError:
        print(f"❌ {ticker}: 컬럼 찾기 실패. (fetch_combined_data 컬럼명 확인 필요)")
        return None, None

    # 지표 계산
    rs_score = calc_weighted_return(t_close) * 100

    current_price = float(t_close.iloc[-1])
    sma200 = float(t_close.rolling(window=200).mean().iloc[-1])
    weekly_return = ((current_price / t_close.iloc[-6]) - 1) * 100

    # ------------------------------------------------------------------
    # 💡 [NEW] VCP (변동성 수축 필터) 계산
    # 최근 20일간의 하루 진폭(고가-저가) 평균이 60일 진폭 평균 대비 75% 이하로 수축했는지 확인
    # ------------------------------------------------------------------
    daily_range = (t_high - t_low) / t_close
    volatility_20d = daily_range.tail(20).mean()
    volatility_60d = daily_range.tail(60).mean()

    is_vcp = 0
    if volatility_60d > 0 and volatility_20d < (volatility_60d * 0.75):
        is_vcp = 1

    # ------------------------------------------------------------------
    # 💡 [NEW] Volume Dry-up (거래량 고갈 필터) 계산
    # 최근 5일 평균 거래량이 50일 평균 거래량의 60% 이하로 씨가 말랐는지 확인
    # ------------------------------------------------------------------
    vol_50d_avg = t_vol.tail(50).mean()
    vol_5d_avg = t_vol.tail(5).mean()

    is_vol_dry = 0
    if vol_50d_avg > 0 and vol_5d_avg < (vol_50d_avg * 0.6):
        is_vol_dry = 1

    # ------------------------------------------------------------------
    # 💡 [NEW] ATR 14일 계산 및 동적 손절선 (Dynamic Risk Management)
    # ------------------------------------------------------------------
    prev_close = t_close.shift(1)
    tr1 = t_high - t_low
    tr2 = (t_high - prev_close).abs()
    tr3 = (t_low - prev_close).abs()

    # 3개 중 가장 큰 값이 True Range
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_14 = float(tr.tail(14).mean())

    # 2-ATR 기준 손절선 계산
    atr_stop_loss = round(current_price - (2 * atr_14), 2)

    # ------------------------------------------------------------------
    # 날짜 포맷 안전하게 처리하기
    # 앞단에서 날짜가 문자열로 넘어오든, datetime으로 넘어오든
    # 무조건 다시 datetime으로 바꾼 뒤 -> YYYY-MM-DD 문자열로 뽑아냅니다.
    # ------------------------------------------------------------------
    latest_date_obj = pd.to_datetime(df.index[-1])
    formatted_date = latest_date_obj.strftime('%Y-%m-%d')
    # ------------------------------------------------------------------

    # [중요] DB 저장용 딕셔너리
    daily_data = {
        "ticker": ticker,
        "date": formatted_date,  # '2026-02-02'
        "open": float(df[f'Open_{ticker}'].iloc[-1]),
        "high": float(df[f'High_{ticker}'].iloc[-1]),
        "low": float(df[f'Low_{ticker}'].iloc[-1]),
        "close": current_price,
        "volume": int(df[f'Volume_{ticker}'].iloc[-1])
    }

    weekly_data = {
        "ticker": ticker,
        "weekly_date": formatted_date,
        "weekly_return": round(float(weekly_return), 2),
        "rs_value": round(float(rs_score), 2),
        "is_above_200ma": 1 if current_price > sma200 else 0,
        "deviation_200ma": round(((current_price / sma200) - 1) * 100, 2),

        # [NEW] 새로 추가된 지표 적재
        "is_vcp": is_vcp,
        "is_vol_dry": is_vol_dry,
        "atr_stop_loss": atr_stop_loss  # [NEW]
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

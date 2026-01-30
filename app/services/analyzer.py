from prefect import task, get_run_logger
from sqlalchemy import text
from app.core.database import get_engine


@task(name="Calculate-Metrics")
def calculate_metrics(df, ticker, benchmark='VTI'):
    if df.empty or len(df) < 252:
        raise ValueError(f"1년치 데이터 없음 : {ticker}")

    def calc_weighted_return(series):
        if len(series) < 252: return 0
        curr = series.iloc[-1]
        r1 = (curr / series.iloc[-63]) - 1
        r2 = (series.iloc[-63] / series.iloc[-126]) - 1
        r3 = (series.iloc[-126] / series.iloc[-189]) - 1
        r4 = (series.iloc[-189] / series.iloc[-252]) - 1
        return (r1 * 0.4) + (r2 * 0.2) + (r3 * 0.2) + (r4 * 0.2)

    t_close = df[f'Close_{ticker}']
    b_close = df[f'Close_{benchmark}']

    rs_score = (calc_weighted_return(t_close) - calc_weighted_return(b_close)) * 100
    current_price = float(t_close.iloc[-1])
    sma200 = float(t_close.rolling(window=200).mean().iloc[-1])
    weekly_return = ((current_price / t_close.iloc[-6]) - 1) * 100

    daily_data = {
        "TICKER": ticker, "DATE": df.index[-1].strftime('%Y%m%d'),
        "OPEN": float(df[f'Open_{ticker}'].iloc[-1]),
        "HIGH": float(df[f'High_{ticker}'].iloc[-1]),
        "LOW": float(df[f'Low_{ticker}'].iloc[-1]),
        "CLOSE": current_price, "VOLUME": int(df[f'Volume_{ticker}'].iloc[-1])
    }
    weekly_data = {
        "TICKER": ticker, "WEEKLY_DATE": df.index[-1].strftime('%Y%m%d'),
        "WEEKLY_RETURN": round(float(weekly_return), 2),
        "RS_VALUE": round(float(rs_score), 2),
        "IS_ABOVE_200MA": 1 if current_price > sma200 else 0,
        "DEVIATION_200MA": round(((current_price / sma200) - 1) * 100, 2)
    }
    return daily_data, weekly_data


@task(name="Update-RS-Indicators")
def update_rs_indicators():
    logger = get_run_logger()
    engine = get_engine()
    with engine.begin() as conn:
        for col in ["RS_RATING REAL", "STOCK_GRADE TEXT", "RS_MOMENTUM REAL"]:
            try:
                conn.execute(text(f"ALTER TABLE PRICE_WEEKLY ADD COLUMN {col}"))
            except Exception:
                pass

        query = text("""
            UPDATE PRICE_WEEKLY
            SET RS_RATING = T.new_rating, RS_MOMENTUM = T.new_momentum,
                STOCK_GRADE = CASE 
                    WHEN T.new_rating >= 90 THEN 'A' WHEN T.new_rating >= 70 THEN 'B'
                    WHEN T.new_rating >= 50 THEN 'C' WHEN T.new_rating >= 30 THEN 'D' ELSE 'E' END
            FROM (
                SELECT TICKER, WEEKLY_DATE,
                    ROUND(PERCENT_RANK() OVER (PARTITION BY WEEKLY_DATE ORDER BY RS_VALUE ASC) * 100) as new_rating,
                    RS_VALUE - LAG(RS_VALUE) OVER (PARTITION BY TICKER ORDER BY WEEKLY_DATE ASC) as new_momentum
                FROM PRICE_WEEKLY
            ) AS T
            WHERE PRICE_WEEKLY.TICKER = T.TICKER AND PRICE_WEEKLY.WEEKLY_DATE = T.WEEKLY_DATE;
        """)
        conn.execute(query)
    logger.info("RS 지표 업데이트 완료")
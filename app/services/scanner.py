import pandas as pd
from sqlalchemy import text
from app.core.database import get_engine
from tabulate import tabulate


def scan_breakout_stocks():
    """
    횡보 후 거래량 실린 상승(박스권 돌파) 종목 스캐닝
    (데이터 개수 부족한 종목 제외 로직 추가)
    """
    engine = get_engine()

    query = text("""
    WITH market_data AS (
        SELECT 
            d.ticker,
            d.date,
            d.close,
            d.volume,
            -- [1] 과거 60일간의 고점 (오늘 제외)
            MAX(d.high) OVER(PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) as box_high,
            -- [2] 과거 60일간의 최저점 (오늘 제외)
            MIN(d.low) OVER(PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) as box_low,
            -- [3] 과거 20일간의 평균 거래량 (오늘 제외)
            AVG(d.volume) OVER(PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as avg_vol_20,
            -- [4] ★ 안전장치: 실제로 참고한 과거 데이터 개수 세기
            COUNT(d.close) OVER(PARTITION BY d.ticker ORDER BY d.date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) as data_count
        FROM price_daily d
        JOIN stock_master m ON d.ticker = m.ticker
        WHERE m.market_type = 'STOCK' 
    ),
    latest_data AS (
        SELECT * FROM market_data
        WHERE date = (SELECT MAX(date) FROM price_daily) -- 가장 최신 날짜만 선택
    )
    SELECT 
        ticker,
        date,
        close,
        box_high,
        ROUND((box_high - box_low) / box_low * 100, 1) as box_width_pct,
        ROUND(volume / avg_vol_20 * 100, 0) as vol_spike_pct,
        data_count
    FROM latest_data
    WHERE 
          -- [조건 0] ★ 데이터가 최소 60개는 있어야 함 (신규 상장주 제외)
          data_count >= 60

          -- [조건 1] 횡보: 고점과 저점 차이가 20% 이내 (박스권)
      AND (box_high - box_low) / box_low <= 0.20

          -- [조건 2] 돌파: 오늘 종가가 박스권 고점 돌파
      AND close > box_high

          -- [조건 3] 거래량 폭발: 20일 평균 대비 300% 이상
      AND volume >= avg_vol_20 * 3.0

          -- [조건 4] 잡주 필터링 (거래대금)
      AND (close * volume) > 1000000 

    ORDER BY vol_spike_pct DESC;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        print("🔍 조건에 맞는 종목이 없습니다.")
        return []

    print(f"\n🚀 [Breakout Scanner] 박스권 돌파 종목 발견: {len(df)}개")
    # 보기 좋게 출력
    print(tabulate(df[['ticker', 'date', 'close', 'box_width_pct', 'vol_spike_pct']],
                   headers=['티커', '날짜', '종가', '박스권폭(%)', '거래량급증(%)'],
                   tablefmt='psql', showindex=False))

    return df.to_dict('records')


if __name__ == "__main__":
    scan_breakout_stocks()
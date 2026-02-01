import yfinance as yf
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from prefect import task, get_run_logger
from app.core.database import get_engine


# ---------------------------------------------------------
# [Core] 분기 실적 처리 (날짜 제한 없이 무조건 Upsert)
# ---------------------------------------------------------
def process_quarterly_data(engine, ticker, stock_obj, logger):
    try:
        fin = stock_obj.quarterly_financials
        if fin.empty: return
    except Exception:
        return

    df = fin.T
    df.index = pd.to_datetime(df.index)

    net_income = df.get('Net Income', pd.Series(dtype=float))
    revenue = df.get('Total Revenue', pd.Series(dtype=float))

    rev_growth = revenue.pct_change(periods=4, fill_method=None) * 100
    ni_growth = net_income.pct_change(periods=4, fill_method=None) * 100

    rows_to_insert = []

    for date_idx, row in df.iterrows():
        current_date = date_idx.date()
        val_revenue = revenue.get(date_idx)
        val_net_income = net_income.get(date_idx)

        if pd.isna(val_revenue) or val_revenue == 0: continue
        if pd.isna(val_net_income): continue

        data = {
            "ticker": ticker,
            "date": current_date,
            "net_income": int(val_net_income),
            "revenue": int(val_revenue),
            "rev_growth_yoy": None if pd.isna(rev_growth.get(date_idx)) else round(float(rev_growth.get(date_idx)), 2),
            "eps_growth_yoy": None if pd.isna(ni_growth.get(date_idx)) else round(float(ni_growth.get(date_idx)), 2)
        }
        rows_to_insert.append(data)

    if rows_to_insert:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO financial_quarterly (ticker, date, net_income, revenue, rev_growth_yoy, eps_growth_yoy)
                VALUES (:ticker, :date, :net_income, :revenue, :rev_growth_yoy, :eps_growth_yoy)
                ON CONFLICT (ticker, date) DO UPDATE SET
                    net_income = EXCLUDED.net_income,
                    revenue = EXCLUDED.revenue,
                    rev_growth_yoy = EXCLUDED.rev_growth_yoy,
                    eps_growth_yoy = EXCLUDED.eps_growth_yoy
            """), rows_to_insert)
        logger.info(f"   └ 📦 {ticker}: 분기 실적 {len(rows_to_insert)}건 동기화")


# ---------------------------------------------------------
# [Core] 연간 실적 처리 (ROE용)
# ---------------------------------------------------------
def process_annual_data(engine, ticker, stock_obj, logger):
    try:
        fin = stock_obj.financials.T
        bal = stock_obj.balance_sheet.T
        if fin.empty or bal.empty: return
    except Exception:
        return

    fin.index = pd.to_datetime(fin.index)
    bal.index = pd.to_datetime(bal.index)

    merged = fin.join(bal, lsuffix='_fin', rsuffix='_bal')

    net_income = merged.get('Net Income', pd.Series(dtype=float))
    equity = merged.get('Stockholders Equity', pd.Series(dtype=float))
    revenue = merged.get('Total Revenue', pd.Series(dtype=float))

    roe_series = (net_income / equity) * 100

    rows_to_insert = []

    for date_idx, row in merged.iterrows():
        current_year = date_idx.year
        val_revenue = revenue.get(date_idx)
        val_net_income = net_income.get(date_idx)

        if pd.isna(val_revenue) or val_revenue == 0: continue

        data = {
            "ticker": ticker,
            "year": current_year,
            "net_income": None if pd.isna(val_net_income) else int(val_net_income),
            "revenue": None if pd.isna(val_revenue) else int(val_revenue),
            "roe": None if pd.isna(roe_series.get(date_idx)) else round(float(roe_series.get(date_idx)), 2)
        }
        rows_to_insert.append(data)

    if rows_to_insert:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO financial_annual (ticker, year, net_income, revenue, roe)
                VALUES (:ticker, :year, :net_income, :revenue, :roe)
                ON CONFLICT (ticker, year) DO UPDATE SET
                    net_income = EXCLUDED.net_income,
                    revenue = EXCLUDED.revenue,
                    roe = EXCLUDED.roe
            """), rows_to_insert)
        logger.info(f"   └ 📅 {ticker}: 연간 실적(ROE) {len(rows_to_insert)}건 동기화")


# ---------------------------------------------------------
# [New] Stock Fundamentals (등급 산정) 처리
# ---------------------------------------------------------
def process_stock_fundamentals(engine, ticker, logger):
    """
    수집된 Quarterly, Annual 데이터를 바탕으로 점수(Grade)를 매겨 stock_fundamentals에 저장
    """
    with engine.connect() as conn:
        # 1. 최신 분기 성장률 가져오기 (가장 최근 날짜 1개)
        q_query = text("""
            SELECT date, eps_growth_yoy, rev_growth_yoy 
            FROM financial_quarterly 
            WHERE ticker = :ticker 
            ORDER BY date DESC LIMIT 1
        """)
        q_data = conn.execute(q_query, {"ticker": ticker}).fetchone()

        # 2. 최신 연간 ROE 가져오기 (가장 최근 연도 1개)
        a_query = text("""
            SELECT roe 
            FROM financial_annual 
            WHERE ticker = :ticker 
            ORDER BY year DESC LIMIT 1
        """)
        a_data = conn.execute(a_query, {"ticker": ticker}).fetchone()

    # 데이터가 없으면 계산 불가 -> 종료
    if not q_data:
        return

    # 3. 점수 계산 (자체 알고리즘)
    # - EPS 성장률: 높을수록 좋음 (30% 이상이면 만점)
    # - ROE: 높을수록 좋음 (17% 이상이면 만점)
    eps_growth = q_data.eps_growth_yoy or 0
    roe = a_data.roe if a_data else 0

    # [점수 산정 로직]
    # 성장률 점수 (최대 60점): 성장률 1%당 2점 (30% 성장 시 60점)
    growth_score = min(max(eps_growth * 2, 0), 60)

    # ROE 점수 (최대 40점): ROE 1%당 2.35점 (17% ROE 시 약 40점)
    roe_score = min(max(roe * 2.35, 0), 40)

    total_score = round(growth_score + roe_score, 1)

    # 4. 등급 부여 (Fundamental Grade)
    if total_score >= 80:
        grade = 'A'
    elif total_score >= 60:
        grade = 'B'
    elif total_score >= 40:
        grade = 'C'
    elif total_score >= 20:
        grade = 'D'
    else:
        grade = 'E'

    # 5. DB Upsert
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO stock_fundamentals (ticker, latest_q_date, fundamental_grade, eps_rating, updated_at)
            VALUES (:ticker, :latest_q_date, :grade, :score, :updated_at)
            ON CONFLICT (ticker) DO UPDATE SET
                latest_q_date = EXCLUDED.latest_q_date,
                fundamental_grade = EXCLUDED.fundamental_grade,
                eps_rating = EXCLUDED.eps_rating,
                updated_at = EXCLUDED.updated_at
        """), {
            "ticker": ticker,
            "latest_q_date": q_data.date,
            "grade": grade,
            "score": total_score,
            "updated_at": datetime.now()
        })

    logger.info(f"   └ 🏆 {ticker}: 펀더멘털 등급 산정 완료 (등급: {grade}, 점수: {total_score})")


# ---------------------------------------------------------
# [Task] 메인 실행 함수
# ---------------------------------------------------------
@task(name="Fetch-Financials")
def fetch_and_save_financials():
    logger = get_run_logger()
    engine = get_engine()

    with engine.connect() as conn:
        query = text("SELECT ticker FROM stock_master WHERE market_type = 'STOCK'")
        tickers = [row.ticker for row in conn.execute(query).fetchall()]

    logger.info(f"💰 재무제표 및 펀더멘털 분석 시작: 총 {len(tickers)}개 종목")

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)

            # 1. 기초 데이터 수집 (Upsert)
            process_quarterly_data(engine, ticker, stock, logger)
            process_annual_data(engine, ticker, stock, logger)

            # 2. [NEW] 수집된 데이터로 등급 산정 (Aggregate)
            process_stock_fundamentals(engine, ticker, logger)

        except Exception as e:
            logger.error(f"❌ {ticker} 처리 실패: {e}")

    logger.info("✅ 모든 재무/펀더멘털 데이터 업데이트 완료")


if __name__ == "__main__":
    from prefect import flow


    @flow(name="Manual-Run")
    def run():
        fetch_and_save_financials()


    run()
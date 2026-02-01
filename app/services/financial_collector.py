import yfinance as yf
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from prefect import task, get_run_logger
from app.core.database import get_engine


# ---------------------------------------------------------
# [Core] 분기 실적 처리 (정렬 로직 추가)
# ---------------------------------------------------------
def process_quarterly_data(engine, ticker, stock_obj, logger):
    try:
        fin = stock_obj.quarterly_financials
        if fin.empty: return
    except Exception:
        return

    df = fin.T
    df.index = pd.to_datetime(df.index)

    # [핵심 수정 1] 날짜 오름차순(과거->현재) 정렬
    # 이게 없으면 pct_change가 엉뚱하게 계산됩니다.
    df = df.sort_index(ascending=True)

    # 1. 데이터 추출
    net_income = df.get('Net Income', pd.Series(dtype=float))
    revenue = df.get('Total Revenue', pd.Series(dtype=float))
    eps_basic = df.get('Basic EPS', pd.Series(dtype=float))

    # 2. 성장률 계산 (YoY) - 이제 정렬되었으므로 정상 작동
    # 데이터가 5개 미만이면 앞쪽은 어쩔 수 없이 NaN이 뜹니다.
    rev_growth = revenue.pct_change(periods=4, fill_method=None) * 100

    if not eps_basic.empty and not eps_basic.isna().all():
        real_eps_growth = eps_basic.pct_change(periods=4, fill_method=None) * 100
    else:
        real_eps_growth = net_income.pct_change(periods=4, fill_method=None) * 100

    rows_to_insert = []

    # 다시 최신순으로 돌면서 저장 (선택 사항이나 디버깅 편의상)
    # iterrows는 순서대로 나오므로 위에서 오름차순 정렬된 상태로 돕니다.
    for date_idx, row in df.iterrows():
        current_date = date_idx.date()

        val_revenue = revenue.get(date_idx)
        val_net_income = net_income.get(date_idx)
        val_eps = eps_basic.get(date_idx)

        # 유효성 검사
        if pd.isna(val_revenue) or val_revenue == 0: continue
        if pd.isna(val_net_income) and pd.isna(val_eps): continue

        # [핵심 수정 2] 성장률이 NaN인 경우(데이터 부족) None으로 명확히 처리
        r_growth_val = rev_growth.get(date_idx)
        e_growth_val = real_eps_growth.get(date_idx)

        data = {
            "ticker": ticker,
            "date": current_date,
            "net_income": int(val_net_income) if not pd.isna(val_net_income) else None,
            "revenue": int(val_revenue),
            "eps_basic": float(val_eps) if not pd.isna(val_eps) else None,

            # NaN 체크를 확실하게 해서 넣음
            "rev_growth_yoy": round(float(r_growth_val), 2) if pd.notna(r_growth_val) else None,
            "eps_growth_yoy": round(float(e_growth_val), 2) if pd.notna(e_growth_val) else None
        }
        rows_to_insert.append(data)

    if rows_to_insert:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO financial_quarterly (
                    ticker, date, net_income, revenue, eps_basic, 
                    rev_growth_yoy, eps_growth_yoy
                )
                VALUES (
                    :ticker, :date, :net_income, :revenue, :eps_basic, 
                    :rev_growth_yoy, :eps_growth_yoy
                )
                ON CONFLICT (ticker, date) DO UPDATE SET
                    net_income = EXCLUDED.net_income,
                    revenue = EXCLUDED.revenue,
                    eps_basic = EXCLUDED.eps_basic,
                    rev_growth_yoy = EXCLUDED.rev_growth_yoy,
                    eps_growth_yoy = EXCLUDED.eps_growth_yoy
            """), rows_to_insert)
        logger.info(f"   └ 📦 {ticker}: 분기 실적(EPS포함) {len(rows_to_insert)}건 동기화")


# ---------------------------------------------------------
# [Core] 연간 실적 처리
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
    eps_basic = merged.get('Basic EPS', pd.Series(dtype=float))  # [NEW]

    roe_series = (net_income / equity) * 100

    rows_to_insert = []

    for date_idx, row in merged.iterrows():
        current_year = date_idx.year
        val_revenue = revenue.get(date_idx)
        val_net_income = net_income.get(date_idx)
        val_eps = eps_basic.get(date_idx)  # [NEW]

        if pd.isna(val_revenue) or val_revenue == 0: continue

        data = {
            "ticker": ticker,
            "year": current_year,
            "net_income": int(val_net_income) if not pd.isna(val_net_income) else None,
            "revenue": int(val_revenue),
            "eps_basic": float(val_eps) if not pd.isna(val_eps) else None,  # [NEW]
            "roe": None if pd.isna(roe_series.get(date_idx)) else round(float(roe_series.get(date_idx)), 2)
        }
        rows_to_insert.append(data)

    if rows_to_insert:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO financial_annual (ticker, year, net_income, revenue, eps_basic, roe)
                VALUES (:ticker, :year, :net_income, :revenue, :eps_basic, :roe)
                ON CONFLICT (ticker, year) DO UPDATE SET
                    net_income = EXCLUDED.net_income,
                    revenue = EXCLUDED.revenue,
                    eps_basic = EXCLUDED.eps_basic,
                    roe = EXCLUDED.roe
            """), rows_to_insert)
        logger.info(f"   └ 📅 {ticker}: 연간 실적(ROE+EPS) {len(rows_to_insert)}건 동기화")


# ---------------------------------------------------------
# [New] Stock Fundamentals (등급 산정 + 지표 저장)
# ---------------------------------------------------------
def process_stock_fundamentals(engine, ticker, logger):
    with engine.connect() as conn:
        # [수정] 이제 eps_basic 컬럼도 가져올 수 있지만,
        # 점수 계산에는 이미 계산된 'eps_growth_yoy'를 쓰면 됩니다.
        q_query = text("""
            SELECT date, eps_growth_yoy, rev_growth_yoy, eps_basic 
            FROM financial_quarterly 
            WHERE ticker = :ticker 
            ORDER BY date DESC LIMIT 1
        """)
        q_data = conn.execute(q_query, {"ticker": ticker}).fetchone()

        a_query = text("""
            SELECT roe 
            FROM financial_annual 
            WHERE ticker = :ticker 
            ORDER BY year DESC LIMIT 1
        """)
        a_data = conn.execute(a_query, {"ticker": ticker}).fetchone()

    if not q_data:
        return

    # 상세 지표 (DB 저장용)
    raw_eps_growth = q_data.eps_growth_yoy
    raw_rev_growth = q_data.rev_growth_yoy
    raw_roe = a_data.roe if a_data else None

    # 점수 계산용 (None -> 0)
    calc_eps_growth = raw_eps_growth if raw_eps_growth is not None else 0.0
    calc_roe = raw_roe if raw_roe is not None else 0.0

    # [점수 알고리즘]
    growth_score = min(max(calc_eps_growth * 2, 0), 60)
    roe_score = min(max(calc_roe * 2.35, 0), 40)
    total_score = round(growth_score + roe_score, 1)

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

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO stock_fundamentals (
                ticker, latest_q_date, fundamental_grade, eps_rating, 
                eps_growth, rev_growth, roe, updated_at
            )
            VALUES (
                :ticker, :latest_q_date, :grade, :score, 
                :eps_growth, :rev_growth, :roe, :updated_at
            )
            ON CONFLICT (ticker) DO UPDATE SET
                latest_q_date = EXCLUDED.latest_q_date,
                fundamental_grade = EXCLUDED.fundamental_grade,
                eps_rating = EXCLUDED.eps_rating,
                eps_growth = EXCLUDED.eps_growth,
                rev_growth = EXCLUDED.rev_growth,
                roe = EXCLUDED.roe,
                updated_at = EXCLUDED.updated_at
        """), {
            "ticker": ticker,
            "latest_q_date": q_data.date,
            "grade": grade,
            "score": total_score,
            "eps_growth": raw_eps_growth,
            "rev_growth": raw_rev_growth,
            "roe": raw_roe,
            "updated_at": datetime.now()
        })

    logger.info(f"   └ 🏆 {ticker}: 등급 {grade} ({total_score}점) | EPS성장 {raw_eps_growth}% (Real EPS)")


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

    logger.info(f"💰 재무제표 수집 시작: 총 {len(tickers)}개 종목")

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            process_quarterly_data(engine, ticker, stock, logger)
            process_annual_data(engine, ticker, stock, logger)
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

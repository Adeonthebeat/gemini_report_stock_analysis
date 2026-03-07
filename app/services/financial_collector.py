from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
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

    # 2. 성장률 계산 (YoY)
    rev_growth = revenue.pct_change(periods=4, fill_method=None) * 100
    # [핵심 수정] 과거 매출이 0이어서 무한대(inf)가 나온 경우 NaN으로 안전하게 치환
    rev_growth = rev_growth.replace([np.inf, -np.inf], np.nan)

    if not eps_basic.empty and not eps_basic.isna().all():
        real_eps_growth = eps_basic.pct_change(periods=4, fill_method=None) * 100
        # [핵심 수정] 무한대 치환
        real_eps_growth = real_eps_growth.replace([np.inf, -np.inf], np.nan)
    else:
        real_eps_growth = net_income.pct_change(periods=4, fill_method=None) * 100
        # [핵심 수정] 무한대 치환
        real_eps_growth = real_eps_growth.replace([np.inf, -np.inf], np.nan)
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
            SELECT date, eps_growth_yoy, rev_growth_yoy, eps_basic, net_income 
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
    latest_eps = q_data.eps_basic
    latest_ni = q_data.net_income
    raw_roe = a_data.roe if a_data else None

    # 점수 계산용 (None -> 0)
    calc_eps_growth = raw_eps_growth if raw_eps_growth is not None else 0.0
    calc_roe = raw_roe if raw_roe is not None else 0.0

    # [점수 알고리즘]
    growth_score = min(max(calc_eps_growth * 2, 0), 60)
    roe_score = min(max(calc_roe * 2.35, 0), 40)
    total_score = round(growth_score + roe_score, 1)

    # ✨ 2. [핵심] 적자 기업 페널티 로직 추가
    is_deficit = False
    if (latest_eps is not None and latest_eps < 0) or (latest_ni is not None and latest_ni < 0):
        is_deficit = True

    if is_deficit:
        # 적자 기업은 최대 39점(D등급 최고점)으로 점수를 제한합니다.
        # (적자폭이 줄어든 턴어라운드 상태라도 C등급 이상은 불가)
        total_score = min(total_score, 39.0)

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

    # 단일 종목 처리 함수 정의
    def process_single_financial(ticker):
        try:
            stock = yf.Ticker(ticker)
            process_quarterly_data(engine, ticker, stock, logger)
            process_annual_data(engine, ticker, stock, logger)
            process_stock_fundamentals(engine, ticker, logger)
            return True, ticker
        except Exception as e:
            return False, f"{ticker} 실패: {e}"

    # 워커 10개를 투입하여 동시 다발적으로 수집
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_single_financial, t): t for t in tickers}

        for future in as_completed(futures):
            success, msg = future.result()
            if not success:
                logger.error(f"❌ {msg}")

    logger.info("✅ 모든 재무/펀더멘털 데이터 업데이트 완료")


@task(name="Bulk-Grade-Update-SQL")
def bulk_update_fundamentals_by_sql():
    logger = get_run_logger()
    engine = get_engine()

    # 파이썬에서 했던 계산식과 적자 페널티를 SQL 쿼리로 완벽히 구현했습니다.
    bulk_sql = text("""
        WITH RankedQ AS (
            SELECT 
                ticker, date, eps_growth_yoy, rev_growth_yoy, eps_basic, net_income,
                ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY date DESC) as rn
            FROM financial_quarterly
        ),
        LatestQ AS (
            SELECT * FROM RankedQ WHERE rn = 1
        ),
        RankedA AS (
            SELECT 
                ticker, roe,
                ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY year DESC) as rn
            FROM financial_annual
        ),
        LatestA AS (
            SELECT * FROM RankedA WHERE rn = 1
        ),
        Calculated AS (
            SELECT 
                q.ticker,
                q.date AS latest_q_date,
                q.eps_growth_yoy AS raw_eps_growth,
                q.rev_growth_yoy AS raw_rev_growth,
                a.roe AS raw_roe,
                q.eps_basic,
                q.net_income,
                -- 1. EPS 성장성 점수 (0~60)
                CASE WHEN COALESCE(q.eps_growth_yoy, 0) * 2 > 60 THEN 60
                     WHEN COALESCE(q.eps_growth_yoy, 0) * 2 < 0 THEN 0
                     ELSE COALESCE(q.eps_growth_yoy, 0) * 2 END AS growth_score,
                -- 2. ROE 수익성 점수 (0~40)
                CASE WHEN COALESCE(a.roe, 0) * 2.35 > 40 THEN 40
                     WHEN COALESCE(a.roe, 0) * 2.35 < 0 THEN 0
                     ELSE COALESCE(a.roe, 0) * 2.35 END AS roe_score
            FROM LatestQ q
            LEFT JOIN LatestA a ON q.ticker = a.ticker
        ),
        Scored AS (
            SELECT 
                ticker,
                latest_q_date,
                raw_eps_growth,
                raw_rev_growth,
                raw_roe,
                -- 3. [핵심] 적자 페널티 적용 (합산 점수가 높아도 최대 39점으로 캡)
                CASE WHEN (eps_basic < 0 OR net_income < 0) AND (growth_score + roe_score) > 39.0 
                     THEN 39.0 
                     ELSE (growth_score + roe_score) 
                END AS total_score
            FROM Calculated
        )
        -- 4. 최종 업데이트 (INSERT ON CONFLICT)
        INSERT INTO stock_fundamentals (
            ticker, latest_q_date, eps_growth, rev_growth, roe, 
            eps_rating, fundamental_grade, updated_at
        )
        SELECT 
            ticker,
            latest_q_date,
            raw_eps_growth,
            raw_rev_growth,
            raw_roe,
            ROUND(CAST(total_score AS NUMERIC), 1),
            CASE 
                WHEN total_score >= 80 THEN 'A'
                WHEN total_score >= 60 THEN 'B'
                WHEN total_score >= 40 THEN 'C'
                WHEN total_score >= 20 THEN 'D'
                ELSE 'E'
            END AS fundamental_grade,
            CURRENT_TIMESTAMP
        FROM Scored
        ON CONFLICT (ticker) DO UPDATE SET
            latest_q_date = EXCLUDED.latest_q_date,
            eps_rating = EXCLUDED.eps_rating,
            fundamental_grade = EXCLUDED.fundamental_grade,
            eps_growth = EXCLUDED.eps_growth,
            rev_growth = EXCLUDED.rev_growth,
            roe = EXCLUDED.roe,
            updated_at = EXCLUDED.updated_at;
    """)

    with engine.begin() as conn:
        result = conn.execute(bulk_sql)
        logger.info(f"✅ DB 쿼리 일괄 실행 완료! 수천 개의 등급이 즉시 재산정되었습니다. (적용된 row 수: {result.rowcount})")


if __name__ == "__main__":
    from prefect import flow


    @flow(name="Manual-Run")
    def run():
        # fetch_and_save_financials()

        bulk_update_fundamentals_by_sql()


    run()

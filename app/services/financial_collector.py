import time

import pandas as pd
import numpy as np
from sqlalchemy import text
from datetime import datetime
from prefect import task, get_run_logger
from app.core.database import get_engine
from yahooquery import Ticker


# ---------------------------------------------------------
# [Core] 분기 실적 처리 (yahooquery DataFrame 슬라이싱 방식)
# ---------------------------------------------------------
def process_quarterly_data(engine, ticker, df_q, logger):
    try:
        if ticker not in df_q.index: return

        df = df_q.loc[ticker].copy()
        if isinstance(df, pd.Series):
            df = df.to_frame().T

        if df.empty: return
    except Exception:
        return

    if 'asOfDate' not in df.columns: return

    df['asOfDate'] = pd.to_datetime(df['asOfDate'])
    df = df.sort_values('asOfDate', ascending=True)
    df = df.set_index('asOfDate')

    net_income = df.get('NetIncome', pd.Series(dtype=float))
    revenue = df.get('TotalRevenue', pd.Series(dtype=float))
    eps_basic = df.get('BasicEPS', pd.Series(dtype=float))

    rev_growth = revenue.pct_change(periods=4, fill_method=None) * 100
    rev_growth = rev_growth.replace([np.inf, -np.inf], np.nan)

    if not eps_basic.empty and not eps_basic.isna().all():
        real_eps_growth = eps_basic.pct_change(periods=4, fill_method=None) * 100
        real_eps_growth = real_eps_growth.replace([np.inf, -np.inf], np.nan)
    else:
        real_eps_growth = net_income.pct_change(periods=4, fill_method=None) * 100
        real_eps_growth = real_eps_growth.replace([np.inf, -np.inf], np.nan)

    rows_to_insert = []
    for date_idx, row in df.iterrows():
        current_date = date_idx.date()
        val_revenue = revenue.get(date_idx)
        val_net_income = net_income.get(date_idx)
        val_eps = eps_basic.get(date_idx)

        if pd.isna(val_revenue) or val_revenue == 0: continue
        if pd.isna(val_net_income) and pd.isna(val_eps): continue

        r_growth_val = rev_growth.get(date_idx)
        e_growth_val = real_eps_growth.get(date_idx)

        data = {
            "ticker": ticker,
            "date": current_date,
            "net_income": int(val_net_income) if pd.notna(val_net_income) else None,
            "revenue": int(val_revenue),
            "eps_basic": float(val_eps) if pd.notna(val_eps) else None,
            "rev_growth_yoy": round(float(r_growth_val), 2) if pd.notna(r_growth_val) else None,
            "eps_growth_yoy": round(float(e_growth_val), 2) if pd.notna(e_growth_val) else None
        }
        rows_to_insert.append(data)

    if rows_to_insert:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO financial_quarterly (
                    ticker, date, net_income, revenue, eps_basic, rev_growth_yoy, eps_growth_yoy
                ) VALUES (
                    :ticker, :date, :net_income, :revenue, :eps_basic, :rev_growth_yoy, :eps_growth_yoy
                ) ON CONFLICT (ticker, date) DO UPDATE SET
                    net_income = EXCLUDED.net_income, revenue = EXCLUDED.revenue,
                    eps_basic = EXCLUDED.eps_basic, rev_growth_yoy = EXCLUDED.rev_growth_yoy,
                    eps_growth_yoy = EXCLUDED.eps_growth_yoy
            """), rows_to_insert)
        logger.info(f"   └ 📦 {ticker}: 분기 실적 갱신")


# ---------------------------------------------------------
# [Core] 연간 실적 처리 (yahooquery 버전)
# ---------------------------------------------------------
def process_annual_data(engine, ticker, df_a_inc, df_a_bal, logger):
    try:
        if ticker not in df_a_inc.index or ticker not in df_a_bal.index: return

        df_inc = df_a_inc.loc[ticker].copy()
        df_bal = df_a_bal.loc[ticker].copy()

        if isinstance(df_inc, pd.Series): df_inc = df_inc.to_frame().T
        if isinstance(df_bal, pd.Series): df_bal = df_bal.to_frame().T

        if df_inc.empty or df_bal.empty: return
    except Exception:
        return

    df_inc['asOfDate'] = pd.to_datetime(df_inc['asOfDate'])
    df_bal['asOfDate'] = pd.to_datetime(df_bal['asOfDate'])

    df_inc = df_inc.set_index('asOfDate')
    df_bal = df_bal.set_index('asOfDate')

    merged = df_inc.join(df_bal, lsuffix='_fin', rsuffix='_bal')

    net_income = merged.get('NetIncome', pd.Series(dtype=float))
    equity = merged.get('StockholdersEquity', merged.get('CommonStockEquity', pd.Series(dtype=float)))
    revenue = merged.get('TotalRevenue', pd.Series(dtype=float))
    eps_basic = merged.get('BasicEPS', pd.Series(dtype=float))

    roe_series = (net_income / equity) * 100

    rows_to_insert = []
    for date_idx, row in merged.iterrows():
        current_year = date_idx.year
        val_revenue = revenue.get(date_idx)
        val_net_income = net_income.get(date_idx)
        val_eps = eps_basic.get(date_idx)

        if pd.isna(val_revenue) or val_revenue == 0: continue

        data = {
            "ticker": ticker,
            "year": current_year,
            "net_income": int(val_net_income) if pd.notna(val_net_income) else None,
            "revenue": int(val_revenue),
            "eps_basic": float(val_eps) if pd.notna(val_eps) else None,
            "roe": None if pd.isna(roe_series.get(date_idx)) else round(float(roe_series.get(date_idx)), 2)
        }
        rows_to_insert.append(data)

    if rows_to_insert:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO financial_annual (ticker, year, net_income, revenue, eps_basic, roe)
                VALUES (:ticker, :year, :net_income, :revenue, :eps_basic, :roe)
                ON CONFLICT (ticker, year) DO UPDATE SET
                    net_income = EXCLUDED.net_income, revenue = EXCLUDED.revenue,
                    eps_basic = EXCLUDED.eps_basic, roe = EXCLUDED.roe
            """), rows_to_insert)
        logger.info(f"   └ 📅 {ticker}: 연간 실적 갱신")


# ---------------------------------------------------------
# [New] Stock Fundamentals (기존 로직 유지)
# ---------------------------------------------------------
def process_stock_fundamentals(engine, ticker, logger):
    with engine.connect() as conn:
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

    raw_eps_growth = q_data.eps_growth_yoy
    raw_rev_growth = q_data.rev_growth_yoy
    raw_roe = a_data.roe if a_data else None

    calc_eps_growth = raw_eps_growth if raw_eps_growth is not None else 0.0
    calc_roe = raw_roe if raw_roe is not None else 0.0

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
            "ticker": ticker, "latest_q_date": q_data.date, "grade": grade, "score": total_score,
            "eps_growth": raw_eps_growth, "rev_growth": raw_rev_growth, "roe": raw_roe,
            "updated_at": datetime.now()
        })


# ---------------------------------------------------------
# [Task] 메인 실행 함수
# ---------------------------------------------------------
@task(name="Fetch-Financials")
def fetch_and_save_financials():
    logger = get_run_logger()
    engine = get_engine()

    with engine.connect() as conn:
        # 스마트 스킵 제거 -> 무조건 STOCK 마켓 타입 전체를 캔다.
        query = text("SELECT ticker FROM stock_master WHERE market_type = 'STOCK'")
        tickers = [row.ticker for row in conn.execute(query).fetchall()]

    if not tickers:
        logger.info("❌ 대상 종목이 없습니다.")
        return

    # 한 번에 요청할 종목 개수 (너무 크면 멈추고, 너무 작으면 느려짐. 50이 황금비율!)
    CHUNK_SIZE = 50
    logger.info(f"💰 yahooquery 벌크 수집 시작: 전체 {len(tickers)}개 종목 (청크 사이즈: {CHUNK_SIZE}개씩 분할)")

    # 쪼개서 받아온 DataFrame들을 임시로 담을 바구니
    all_is_q, all_is_a, all_bs_a = [], [], []

    # 1. 50개씩 잘라서 야후 서버에 요청
    for i in range(0, len(tickers), CHUNK_SIZE):
        chunk_tickers = tickers[i: i + CHUNK_SIZE]
        logger.info(f"   - 🔄 진행 중: {i + 1} ~ {min(i + CHUNK_SIZE, len(tickers))} / {len(tickers)}")

        # 50개만 비동기로 요청
        yq_tickers = Ticker(chunk_tickers, asynchronous=True)

        try:
            q_inc = yq_tickers.income_statement('q')
            a_inc = yq_tickers.income_statement('a')
            a_bal = yq_tickers.balance_sheet('a')

            # 정상적으로 DataFrame이 반환되었을 때만 바구니에 담기
            if isinstance(q_inc, pd.DataFrame): all_is_q.append(q_inc)
            if isinstance(a_inc, pd.DataFrame): all_is_a.append(a_inc)
            if isinstance(a_bal, pd.DataFrame): all_bs_a.append(a_bal)
        except Exception as e:
            logger.error(f"❌ 청크 수집 중 에러 발생: {e}")

        # [핵심] 야후 서버가 차단하지 않도록 1.5초 휴식 (매너 타임)
        time.sleep(1.5)

    logger.info("📥 다운로드 완료! 데이터를 병합합니다.")

    # 2. 바구니에 담긴 50개짜리 조각들을 하나의 거대한 판판이(DataFrame)로 합치기
    df_is_q = pd.concat(all_is_q) if all_is_q else pd.DataFrame()
    df_is_a = pd.concat(all_is_a) if all_is_a else pd.DataFrame()
    df_bs_a = pd.concat(all_bs_a) if all_bs_a else pd.DataFrame()

    # 3. 합쳐진 데이터를 종목별로 순회하며 DB에 꽂기
    for ticker in tickers:
        try:
            process_quarterly_data(engine, ticker, df_is_q, logger)
            process_annual_data(engine, ticker, df_is_a, df_bs_a, logger)
            process_stock_fundamentals(engine, ticker, logger)
        except Exception as e:
            logger.error(f"❌ {ticker} 처리 실패: {e}")

    logger.info("✅ 전체 재무/펀더멘털 데이터 강제 업데이트 완료")


if __name__ == "__main__":
    from prefect import flow


    @flow(name="Manual-Run")
    def run():
        fetch_and_save_financials()


    run()
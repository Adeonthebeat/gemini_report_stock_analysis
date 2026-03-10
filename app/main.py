import logging
import warnings

import pandas as pd
import yfinance as yf
from datetime import datetime

warnings.filterwarnings("ignore", message=".*Python version 3.9.*")
warnings.filterwarnings("ignore", category=FutureWarning)

logging.getLogger('yfinance').setLevel(logging.CRITICAL)

from prefect import flow, get_run_logger

from app.services.db_ops import get_tickers, get_finished_tickers, save_to_sqlite_bulk
from app.services.data_fetcher import check_market_data_update, fetch_combined_data, fetch_benchmark_data
from app.services.analyzer import calculate_metrics, update_rs_indicators
from app.services.reporting import generate_ai_report
from app.services.financial_collector import fetch_and_save_financials


@flow(name="Main-Stock-Pipeline")
def stock_analysis_pipeline():
    logger = get_run_logger()

    # 1. 업데이트 필요 여부 확인 (VTI 기준)
    if check_market_data_update('VTI'):
        logger.info("✅ 이미 최신 데이터가 존재합니다. 데이터 수집을 건너뛰고 리포트를 생성합니다.")
        generate_ai_report()
        return

    # ----------------------------------------------------------------
    # [NEW] 기준 날짜(Target Date) 구하기
    # ----------------------------------------------------------------
    try:
        vti_check = yf.download('VTI', period='5d', progress=False, auto_adjust=True)
        target_date_str = vti_check.index[-1].date().strftime('%Y-%m-%d')
        logger.info(f"📅 이번 작업의 기준 날짜(Target Date): {target_date_str}")

        finished_tickers = get_finished_tickers(target_date_str)
        logger.info(f"💾 이미 저장 완료된 종목 수: {len(finished_tickers)}개")

    except Exception as e:
        logger.error(f"❌ 기준 날짜 확인 실패: {e}")
        return

    # 2. 대상 티커 조회
    try:
        ticker_list = get_tickers()
        symbols = [item['ticker'] for item in ticker_list]
        logger.info(f"📋 [티커 로드 완료] 총 {len(symbols)}개 종목을 분석합니다.")

    except Exception as e:
        logger.error(f"❌ 티커 리스트 로드 실패: {e}")
        return

    # 3. 재무데이터 수집 (월말 마지막 영업일 여부 확인)
    today = datetime.now().date()
    last_b_day = (pd.Timestamp(today) + pd.offsets.BMonthEnd(0)).date()

    if today == last_b_day:
        logger.info(f"📅 오늘은 월말 마지막 영업일({today})입니다! 재무 데이터를 갱신합니다.")
        try:
            fetch_and_save_financials()
        except Exception as e:
            logger.error(f"❌ 재무제표 업데이트 중 오류 발생: {e}")
    else:
        logger.info(f"⏩ 오늘은 영업일 중({today})이며, 월말({last_b_day})이 아니므로 재무 수집을 건너뜁니다.")

    # ----------------------------------------------------------------
    # [구조 개선] 4. 데이터 수집 루프 전 벤치마크 단 1회 미리 로드
    # ----------------------------------------------------------------
    logger.info("🌐 벤치마크(VTI) 데이터 1회 사전 캐싱 중 (API 최적화)...")
    try:
        benchmark_df = fetch_benchmark_data('VTI')
    except Exception as e:
        logger.error(f"❌ 벤치마크 로드 실패로 파이프라인 중단: {e}")
        return

    logger.info("🚀 가격 데이터 수집 및 지표 계산을 시작합니다...")

    # 1. 단일 종목 처리 함수 (워커가 할 일)
    def process_ticker(row):
        ticker = row['ticker']
        market_type = row.get('market_type', 'STOCK')
        try:
            df = fetch_combined_data(ticker, benchmark_df, market_type)

            if df is None or df.empty:
                # 🚀 실패 사유를 문자로 명시해서 반환
                return False, ticker, None, None, "데이터 다운로드 실패 (df.empty)"

            daily, weekly = calculate_metrics.fn(df, ticker)

            if daily is None or weekly is None:
                # 🚀 계산 실패 사유 반환
                return False, ticker, None, None, "지표 계산 실패 (calculate_metrics 반환값 None)"

            return True, ticker, daily, weekly, "성공"

        except Exception as e:
            # 🚀 진짜 코드 에러 반환
            return False, ticker, None, None, f"실행 중 에러 발생: {e}"

    logger.info("🚀 가격 데이터 수집 및 지표 계산을 시작합니다...")

    # 🧺 빈 바구니 준비 (여기서 리스트가 초기화됩니다)
    daily_bulk_data = []
    weekly_bulk_data = []

    success_count = 0
    fail_count = 0
    skip_count = 0

    # ----------------------------------------------------------------
    # 🚀 [수정] ThreadPoolExecutor 대신 안전한 순차(Sequential) 수집
    # ----------------------------------------------------------------
    logger.info("🚀 [1단계] 데이터를 하나씩 안전하게 순차적으로 수집합니다 (봇 탐지 방지)...")

    total_tickers = len(ticker_list)
    processed_count = 0

    import time  # 순차 대기용

    for row in ticker_list:

        # 🚀 1. 먼저 row에서 ticker를 꺼내서 정의해줘야 합니다! (이게 없으면 에러남)
        ticker = row['ticker']

        if ticker in finished_tickers:
            skip_count += 1
            processed_count += 1

            # 10개마다 진행 상황 출력 (스킵할 때도 로그는 찍히게)
            if processed_count % 10 == 0 or processed_count == total_tickers:
                logger.info(f"⏳ 수집 진행 중... {processed_count} / {total_tickers} 완료 (성공: {success_count}, 스킵: {skip_count}, 실패: {fail_count})")
            continue  # 아래 다운로드 과정을 무시하고 다음 종목으로 넘어감

        # 워커(일꾼) 대신 메인 스레드가 직접 하나씩 함수를 실행합니다.
        success, ticker, daily, weekly, reason = process_ticker(row)

        if success:
            # 🧺 성공한 데이터를 바구니에 담습니다!
            daily_bulk_data.append(daily)
            weekly_bulk_data.append(weekly)
            success_count += 1

        else:
            fail_count += 1
            logger.error(f"🚨 [{ticker}] 실패 사유: {reason}")

        processed_count += 1

        # 10개마다 진행 상황 출력
        if processed_count % 10 == 0 or processed_count == total_tickers:
            logger.info(
                f"⏳ 수집 진행 중... {processed_count} / {total_tickers} 완료 (성공: {success_count}, 실패: {fail_count})"
            )

    # ----------------------------------------------------------------
    # 🚀 2단계: 바구니에 담긴 데이터를 DB에 일괄 저장 (벌크 인서트)
    # ----------------------------------------------------------------
    if daily_bulk_data and weekly_bulk_data:
        logger.info(f"💾 계산 완료된 {success_count}개 데이터를 DB에 일괄 저장합니다 (Bulk Insert)...")
        try:
            # 🧺 여기서 아까 채운 바구니를 DB 저장 함수로 넘겨줍니다!
            save_to_sqlite_bulk(daily_bulk_data, weekly_bulk_data)
        except Exception as e:
            logger.error(f"❌ DB 벌크 저장 실패: {e}")

    # 결과 요약
    logger.info(f"📈 작업 완료 Summary")
    logger.info(f"   - 성공(신규/갱신): {success_count}")
    logger.info(f"   - 스킵(이미완료): {skip_count}")
    logger.info(f"   - 실패: {fail_count}")

    # 5. 후처리 및 리포트
    logger.info("📊 RS 지표 업데이트 및 리포트 작성을 시작합니다.")

    try:
        update_rs_indicators()
    except Exception as e:
        logger.error(f"❌ RS 지표 업데이트 실패: {e}")

    generate_ai_report()


if __name__ == "__main__":
    stock_analysis_pipeline()
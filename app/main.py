# [여기부터 복사하세요]
import warnings
from datetime import datetime

# "Python version 3.9" 관련 경고 무시하기
warnings.filterwarnings("ignore", message=".*Python version 3.9.*")
warnings.filterwarnings("ignore", category=FutureWarning)

from prefect import flow, get_run_logger

# 모듈화된 Task들 가져오기
from app.services.db_ops import get_tickers, save_to_sqlite
from app.services.data_fetcher import check_market_data_update, fetch_combined_data
from app.services.analyzer import calculate_metrics, update_rs_indicators
from app.services.reporting import generate_ai_report
from app.services.financial_collector import fetch_and_save_financials


@flow(name="Main-Stock-Pipeline")
def stock_analysis_pipeline():
    logger = get_run_logger()

    # 2. 업데이트 필요 여부 확인
    if check_market_data_update('VTI'):
        logger.info("이미 최신 데이터가 존재합니다. 작업을 건너뜁니다.")
        generate_ai_report()
        return

    # 3. 대상 티커 조회
    try:
        ticker_list = get_tickers()
    except Exception:
        logger.error("티커 리스트 로드 실패")
        return

    # 4. 데이터 수집 및 지표 계산 루프
    for row in ticker_list:

        ticker = row['ticker']
        market_type = row.get('market_type', 'STOCK')

        try:
            df = fetch_combined_data(ticker, market_type)
            if df.empty: continue

            daily, weekly = calculate_metrics(df, ticker)
            save_to_sqlite(daily, weekly)
        except Exception as e:
            logger.error(f"Error {ticker}: {e}")

        # [수정] 4. 재무데이터 수집 (매일 하지 말고, "토요일"에만 수행)
        # 0:월, 1:화, ..., 4:금, 5:토, 6:일
        # 한국 시간 기준 화~토 아침에 도니까, 토요일(5)이나 일요일(6)에 잡으면 됩니다.

        today_weekday = datetime.now().weekday()

        # 토요일(5)이거나, 강제로 돌리고 싶을 때만 실행
        if today_weekday == 5:
            logger.info("📅 오늘은 토요일! 재무제표/펀더멘털 데이터를 갱신합니다.")
            fetch_and_save_financials()
        else:
            logger.info("⏩ 평일이므로 재무제표 수집은 건너뜁니다. (토요일에 수행)")

    # 5. 후처리 및 리포트
    update_rs_indicators()
    generate_ai_report()


if __name__ == "__main__":
    stock_analysis_pipeline()

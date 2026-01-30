# [여기부터 복사하세요]
import warnings
# "Python version 3.9" 관련 경고 무시하기
warnings.filterwarnings("ignore", message=".*Python version 3.9.*")
warnings.filterwarnings("ignore", category=FutureWarning)
# [여기까지 복사]

from prefect import flow, get_run_logger
from prefect.client.schemas.schedules import CronSchedule

# 모듈화된 Task들 가져오기
from app.services.db_ops import init_db_flow, get_tickers, save_to_sqlite
from app.services.data_fetcher import check_market_data_update, fetch_combined_data
from app.services.analyzer import calculate_metrics, update_rs_indicators
from app.services.reporting import generate_ai_report, send_email
from app.utils.check_models import checkModels


# from app.services.financials import financial_pipeline # 필요시 import

@flow(name="Main-Stock-Pipeline")
def stock_analysis_pipeline():
    logger = get_run_logger()

    # 2. 업데이트 필요 여부 확인
    if check_market_data_update('VTI'):
        logger.info("이미 최신 데이터가 존재합니다. 작업을 건너뜁니다.")
        return

    # 3. 대상 티커 조회
    try:
        ticker_list = get_tickers()
    except Exception:
        logger.error("티커 리스트 로드 실패")
        return

    # 4. 데이터 수집 및 지표 계산 루프
    for ticker in ticker_list:
        try:
            df = fetch_combined_data(ticker)
            if df.empty: continue

            daily, weekly = calculate_metrics(df, ticker)
            save_to_sqlite(daily, weekly)
        except Exception as e:
            logger.error(f"Error {ticker}: {e}")

    # 5. 후처리 및 리포트
    update_rs_indicators()
    generate_ai_report()


if __name__ == "__main__":
    # 스케줄 설정: 화~토 오전 9:30 (KST)
    stock_analysis_pipeline()

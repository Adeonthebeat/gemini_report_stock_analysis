# [최종 점검 완료 코드]
import warnings
from datetime import datetime

# 경고 메시지 제어
warnings.filterwarnings("ignore", message=".*Python version 3.9.*")
warnings.filterwarnings("ignore", category=FutureWarning)

from prefect import flow, get_run_logger

# 모듈 가져오기 (경로가 맞는지 확인 필요)
from app.services.db_ops import get_tickers, save_to_sqlite
from app.services.data_fetcher import check_market_data_update, fetch_combined_data
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

    # 2. 대상 티커 조회
    try:
        ticker_list = get_tickers()
        
        # 티커 리스트 로깅
        symbols = [item['ticker'] for item in ticker_list]
        logger.info(f"📋 [티커 로드 완료] 총 {len(symbols)}개 종목을 분석합니다.")
        logger.info(f"대상: {', '.join(symbols)}")
        
    except Exception as e:
        logger.error(f"❌ 티커 리스트 로드 실패: {e}")
        return

    # 3. 재무데이터 수집 (토요일에만 한 번 실행)
    # 0:월, 1:화, ..., 4:금, 5:토, 6:일
    today_weekday = datetime.now().weekday()

    if today_weekday == 5:
        logger.info("📅 오늘은 토요일! 재무제표/펀더멘털 데이터를 전체 갱신합니다.")
        try:
            fetch_and_save_financials()
        except Exception as e:
            # 재무제표 실패해도 가격 수집은 계속 진행
            logger.error(f"❌ 재무제표 업데이트 중 오류 발생 (계속 진행합니다): {e}")
    else:
        logger.info(f"⏩ 평일(요일코드: {today_weekday})이므로 재무제표 수집은 건너뜁니다.")

    # 4. 데이터 수집 및 지표 계산 루프
    logger.info("🚀 가격 데이터 수집 및 지표 계산을 시작합니다...")
    
    success_count = 0
    fail_count = 0

    for row in ticker_list:
        ticker = row['ticker']
        market_type = row.get('market_type', 'STOCK')

        # 진행 상황을 보기 좋게 표시
        logger.info(f"🔄 처리 중: {ticker} ({market_type})")
        
        try:
            df = fetch_combined_data(ticker, market_type)
            if df.empty: 
                logger.warning(f"⚠️ {ticker}: 수집된 데이터가 없습니다. (Skip)")
                fail_count += 1
                continue

            daily, weekly = calculate_metrics(df, ticker)
            
            if daily is None or weekly is None:
                logger.warning(f"⚠️ {ticker}: 지표 계산 실패 (데이터 부족 등).")
                fail_count += 1
                continue

            save_to_sqlite(daily, weekly)
            success_count += 1
            
        except Exception as e:
            logger.error(f"❌ {ticker} 처리 중 에러: {e}")
            fail_count += 1

    logger.info(f"📈 데이터 수집 완료 (성공: {success_count}, 실패: {fail_count})")

    # 5. 후처리 및 리포트
    logger.info("📊 RS 지표 업데이트 및 리포트 작성을 시작합니다.")
    try:
        update_rs_indicators()
    except Exception as e:
        logger.error(f"❌ RS 지표 업데이트 실패: {e}")

    generate_ai_report()


if __name__ == "__main__":
    stock_analysis_pipeline()

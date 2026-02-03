# [수정된 app/main.py]
import warnings
import yfinance as yf # 날짜 확인용
from datetime import datetime

warnings.filterwarnings("ignore", message=".*Python version 3.9.*")
warnings.filterwarnings("ignore", category=FutureWarning)

from prefect import flow, get_run_logger

# [수정] get_finished_tickers 추가 임포트
from app.services.db_ops import get_tickers, save_to_sqlite, get_finished_tickers
from app.services.data_fetcher import check_market_data_update, fetch_combined_data
from app.services.analyzer import calculate_metrics, update_rs_indicators
from app.services.reporting import generate_ai_report
from app.services.financial_collector import fetch_and_save_financials


@flow(name="Main-Stock-Pipeline")
def stock_analysis_pipeline():
    logger = get_run_logger()

    # 1. 업데이트 필요 여부 확인 (VTI 기준)
    # 여기서 VTI가 업데이트가 안 되어 있다면 전체 스킵
    if check_market_data_update('VTI'):
        logger.info("✅ 이미 최신 데이터가 존재합니다. 데이터 수집을 건너뛰고 리포트를 생성합니다.")
        generate_ai_report()
        return

    # ----------------------------------------------------------------
    # [NEW] 기준 날짜(Target Date) 구하기
    # VTI 데이터를 살짝 조회해서 "오늘 처리해야 할 날짜"가 며칠인지 확인합니다.
    # ----------------------------------------------------------------
    try:
        # VTI의 최신 날짜를 가져옴 (이 날짜가 곧 우리가 DB에 넣어야 할 날짜)
        vti_check = yf.download('VTI', period='5d', progress=False, auto_adjust=True)
        target_date_str = vti_check.index[-1].date().strftime('%Y-%m-%d')
        logger.info(f"📅 이번 작업의 기준 날짜(Target Date): {target_date_str}")
        
        # [NEW] 이미 DB에 저장된 티커 목록 가져오기 (배치 조회)
        finished_tickers = get_finished_tickers(target_date_str)
        logger.info(f"💾 이미 저장 완료된 종목 수: {len(finished_tickers)}개")
        
    except Exception as e:
        logger.error(f"❌ 기준 날짜 확인 실패: {e}")
        return
    # ----------------------------------------------------------------

    # 2. 대상 티커 조회
    try:
        ticker_list = get_tickers()
        
        symbols = [item['ticker'] for item in ticker_list]
        logger.info(f"📋 [티커 로드 완료] 총 {len(symbols)}개 종목을 분석합니다.")
        
    except Exception as e:
        logger.error(f"❌ 티커 리스트 로드 실패: {e}")
        return

    # 3. 재무데이터 수집 (토요일)
    today_weekday = datetime.now().weekday()
    if today_weekday == 5:
        logger.info("📅 오늘은 토요일! 재무제표/펀더멘털 데이터를 전체 갱신합니다.")
        try:
            fetch_and_save_financials()
        except Exception as e:
            logger.error(f"❌ 재무제표 업데이트 중 오류 발생: {e}")
    else:
        logger.info(f"⏩ 평일(요일코드: {today_weekday})이므로 재무제표 수집은 건너뜁니다.")

    # 4. 데이터 수집 루프
    logger.info("🚀 가격 데이터 수집 및 지표 계산을 시작합니다...")
    
    success_count = 0
    fail_count = 0
    skip_count = 0  # 스킵 카운트 추가

    for row in ticker_list:
        ticker = row['ticker']
        market_type = row.get('market_type', 'STOCK')

        try:
            df = fetch_combined_data(ticker, market_type)

            if df.empty:
                logger.warning(f"⚠️ {ticker}: 수집된 데이터 없음 (Skip)")
                fail_count += 1
                continue

            daily, weekly = calculate_metrics(df, ticker)
            
            if daily is None or weekly is None:
                logger.warning(f"⚠️ {ticker}: 지표 계산 실패")
                fail_count += 1
                continue

            save_to_sqlite(daily, weekly)
            success_count += 1
            
        except Exception as e:
            logger.error(f"❌ {ticker} 처리 중 에러: {e}")
            fail_count += 1

    # 결과 요약에 Skip 정보 추가
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

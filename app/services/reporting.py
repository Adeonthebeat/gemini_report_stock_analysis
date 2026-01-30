import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import markdown
import pandas as pd
from google import genai  # [변경] 새로운 라이브러리 임포트
from datetime import datetime, timedelta

from jinja2 import Environment, FileSystemLoader
from prefect import task, get_run_logger
from sqlalchemy import text
from app.core.database import get_engine
from app.core.config import GOOGLE_API_KEY, BASE_DIR


@task(name="Generate-AI-Report")
def generate_ai_report():
    logger = get_run_logger()
    engine = get_engine()

    # API 키 확인
    if not GOOGLE_API_KEY:
        logger.error("GOOGLE_API_KEY가 설정되지 않았습니다.")
        return

    # [변경] 최신 방식: 클라이언트 인스턴스 직접 생성
    client = genai.Client(api_key=GOOGLE_API_KEY)

    # 상위 종목 추출 쿼리 (기존과 동일)
    query = text("""
        SELECT m.NAME, w.TICKER, w.RS_RATING, f.FUNDAMENTAL_GRADE, 
               f.EPS_RATING, w.WEEKLY_RETURN, w.DEVIATION_200MA
        FROM PRICE_WEEKLY w
        JOIN STOCK_MASTER m ON w.TICKER = m.TICKER
        LEFT JOIN STOCK_FUNDAMENTALS f ON w.TICKER = f.TICKER    
        WHERE w.WEEKLY_DATE = (SELECT MAX(WEEKLY_DATE) FROM PRICE_WEEKLY)
          AND w.RS_RATING >= 90 AND f.FUNDAMENTAL_GRADE = 'A'
          AND w.WEEKLY_RETURN > 0
        ORDER BY w.RS_RATING DESC LIMIT 30
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        logger.warning("분석할 상위 종목이 없습니다.")
        return

    # Gemini 분석 요청 프롬프트
    data_table = df.to_markdown(index=False)
    prompt = f"""
    당신은 윌리엄 오닐 스타일과 드러켄밀러 스타일을 지닌 퀀트 애널리스트입니다. 
    
    다음 슈퍼 주도주 데이터를 윌리엄오닐 스타일과 드러켄밀러 스타일로 나눠서 분석해 주세요:
    
    {data_table}

    ## 시장 주도 테마
    ## Top 5 추천 종목 (티커, 이유 포함, 각 종목의 산업 분석 포함)
    ## 기술적 분석을 통한 신규매수 가격 추천
    ## 각 종목 관련 뉴스 / 커뮤니티를 요약 
    ## 리스크 점검
    ## 미국 주식 시장 요약
    """

    try:
        # 모델명은 본인이 성공했던 것 사용 (예: gemini-flash-latest)
        response = client.models.generate_content(
            model='gemini-flash-latest',
            contents=prompt
        )

        report_content = response.text

        # 3. 화면 출력
        print("\n" + "=" * 80 + "\n🤖 [Gemini AI 리포트]\n" + "=" * 80 + "\n" + report_content)

        # 4. 파일 저장 (어제 날짜)
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        email_subject = f"📈 [Morning Brief] {yesterday} 주도주 분석 리포트"

        # 이메일 발송
        send_email(email_subject, report_content, yesterday)

    except Exception as e:
        logger.error(f"Gemini 분석 오류: {e}")


def send_email(subject, markdown_content, report_date):

    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

    """
    HTML 템플릿을 사용하여 예쁜 뉴스레터를 보내는 함수
    """
    if not EMAIL_USER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        print("⚠️ 이메일 설정 누락")
        return

    try:
        # 1. 마크다운을 순수 HTML 태그로 변환 (표 기능 포함)
        html_body = markdown.markdown(markdown_content, extensions=['tables'])

        # 2. Jinja2 템플릿 로드
        template_dir = os.path.join(BASE_DIR, "app", "templates")  # 템플릿 폴더 경로
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template('newsletter.html')

        # 3. 템플릿에 데이터 채워 넣기 (렌더링)
        final_html = template.render(
            date=report_date,
            body_content=html_body
        )

        # 4. 이메일 구성
        msg = MIMEMultipart('alternative')
        msg['From'] = f"AdeStock Bot <{EMAIL_USER}>"  # 보낸 사람 이름 설정 가능
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = subject

        # HTML 본문 첨부
        msg.attach(MIMEText(final_html, 'html', 'utf-8'))

        # 5. 전송
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)

        print(f"📧 뉴스레터 발송 완료!")

    except Exception as e:
        print(f"❌ 이메일 발송 실패: {e}")
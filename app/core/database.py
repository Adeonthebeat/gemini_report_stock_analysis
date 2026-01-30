import os
from sqlalchemy import create_engine
from app.core.config import BASE_DIR


def get_engine():
    DATABASE_URL = os.getenv("DATABASE_URL")

    # A. 클라우드 DB 주소가 있으면? (GitHub Actions 환경) -> 거기로 연결!
    if DATABASE_URL:
        # SQLAlchemy는 'postgres://' 대신 'postgresql://'을 좋아합니다. (호환성 처리)
        url = DATABASE_URL
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        return create_engine(url)

    # B. 없으면? (내 컴퓨터 테스트 환경) -> 그냥 파일(SQLite) 사용
    else:
        # data 폴더가 없으면 에러 날 수 있으니 만들어줍니다.
        db_path = os.path.join(BASE_DIR, "data", "my_stock_data.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        return create_engine(f"sqlite:///{db_path}")
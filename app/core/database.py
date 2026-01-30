from sqlalchemy import create_engine
from app.core.config import DB_URL

def get_engine():
    # config.py에서 설정한 정확한 경로의 DB를 엽니다.
    return create_engine(DB_URL)
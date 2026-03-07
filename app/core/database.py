import os
from sqlalchemy import create_engine
from app.core.config import BASE_DIR

# 로컬 SQLite fallback (기존 코드 유지)
from app.core.config import DB_URL

def get_engine():
    DATABASE_URL = os.getenv("DATABASE_URL")

    if DATABASE_URL:
        url = DATABASE_URL
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)

        # 🚀 퀀트 개발자의 필수 세팅: Supabase 등 클라우드 DB 연결 안정화
        return create_engine(
            url,
            pool_pre_ping=True,  # 쿼리 실행 전 연결이 살아있는지 확인
            pool_recycle=300,  # 5분(300초)마다 커넥션 재생성 (Supabase 연결 끊김 방지)
            pool_timeout=30,  # 풀에서 커넥션을 가져올 때 최대 대기 시간 (무한 대기 방지)
            max_overflow=5,  # 최대 초과 생성 커넥션 수
            connect_args={
                'connect_timeout': 10  # DB 서버 접속 자체의 타임아웃
            }
        )

    return create_engine(DB_URL)

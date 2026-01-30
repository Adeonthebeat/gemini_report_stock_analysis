import os
from pathlib import Path
from dotenv import load_dotenv

# 프로젝트의 최상위 루트 경로를 자동으로 찾습니다.
# (이 파일의 위치: adeStock/app/core/config.py -> 부모의 부모가 루트)
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# .env 파일 로드
load_dotenv(os.path.join(BASE_DIR, ".env"))

# DB 파일 경로 설정 (data 폴더 안을 가리킴)
DB_PATH = os.path.join(BASE_DIR, "data", "my_stock_data.db")
DB_URL = f"sqlite:///{DB_PATH}?check_same_thread=False"

# API 키 등
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
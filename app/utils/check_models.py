from google import genai
import os
from dotenv import load_dotenv


def checkModels():
    load_dotenv()

    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("API Key가 없습니다.")
    else:
        client = genai.Client(api_key=api_key)
        print("📋 사용 가능한 모델 목록 (전체 출력):")

        try:
            # 필터링 없이 있는 그대로 출력합니다.
            for model in client.models.list():
                print(f"- {model.name}")

        except Exception as e:
            print(f"목록 조회 실패: {e}")
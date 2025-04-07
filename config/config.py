import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# Supabase 설정
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# TQQQ 텔레그램 봇 설정
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 코스피/코스닥 텔레그램 봇 설정
KOSPI_TELEGRAM_BOT_TOKEN = os.getenv("KOSPI_TELEGRAM_BOT_TOKEN")
KOSPI_TELEGRAM_CHAT_ID = os.getenv("KOSPI_TELEGRAM_CHAT_ID")

# 실행 시간 설정
TQQQ_EXECUTION_TIME = os.getenv("TQQQ_EXECUTION_TIME", "09:00")  # 오전 9시, 24시간 형식
KOSPI_EXECUTION_TIME = os.getenv("KOSPI_EXECUTION_TIME", "17:00")  # 오후 5시, 24시간 형식 
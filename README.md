# 텔레그램 주식 알림 봇

텔레그램을 통해 매일 주식 정보를 알려주는 봇입니다.

## 주요 기능

1. **TQQQ 알림 (오전 9시)**
   - TQQQ 종가 정보 제공
   - 200일 이동평균선과 10% 엔벨로프 기준 투자 추천
   - 차트 이미지 생성 및 전송

2. **주식 검색 (오후 5시)**
   - **급등주 포착:** 거래량 급증, 이동평균선 전환, 기관 매수 종목 검색
   - **파동주 분석:** 피보나치 되돌림, 볼린저 밴드, RSI, MACD 등을 활용한 파동 분석

3. **데이터 저장**
   - Supabase를 활용한 주식 데이터 저장 및 관리
   - 날짜별 데이터 축적

## 설치 방법

### 1. 환경 설정

```bash
# 저장소 클론
git clone https://github.com/yourusername/telebot.git
cd telebot

# 가상 환경 생성 및 활성화 (선택 사항)
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt

# 환경 변수 설정
cp .env.example .env
# .env 파일을 편집하여 API 키 등 설정
```

### 2. Supabase 설정

1. [Supabase](https://supabase.io/) 계정 생성
2. 새 프로젝트 생성
3. 프로젝트 URL과 API 키를 `.env` 파일에 입력
4. 데이터베이스 초기화: `python -m telebot.main --init-db`

### 3. 텔레그램 봇 설정

1. [BotFather](https://t.me/botfather)를 통해 텔레그램 봇 생성
2. 봇 토큰 발급 및 `.env` 파일에 입력
3. 봇과 대화를 시작하고 채팅 ID 확인 후 `.env` 파일에 입력

## 사용 방법

### 스케줄러 실행

```bash
python -m telebot.main
```

### 특정 분석만 실행

```bash
# TQQQ 분석 실행
python -m telebot.main --run tqqq

# 급등주 포착 실행
python -m telebot.main --run potential

# 파동주 분석 실행
python -m telebot.main --run wave

# 모든 분석 실행
python -m telebot.main --run all
```

## 프로젝트 구조

```
telebot/
├── config/         # 환경 설정
├── data/           # 데이터 파일
├── models/         # 데이터 모델
├── scripts/        # 분석 스크립트
│   ├── tqqq_analysis.py           # TQQQ 분석
│   ├── potential_stock_finder.py  # 급등주 포착
│   └── wave_analysis.py           # 파동주 분석
├── utils/          # 유틸리티 함수
│   ├── database.py         # 데이터베이스 연결
│   └── telegram_service.py # 텔레그램 메시지 전송
├── .env.example    # 환경 변수 예제
├── main.py         # 메인 스크립트
└── requirements.txt # 의존성 목록
```

## GitHub Actions를 통한 자동화

GitHub Actions를 설정하여 매일 정해진 시간에 자동으로 스크립트를 실행할 수 있습니다.
`.github/workflows/schedule.yml` 파일 예시:

```yaml
name: Stock Alert Scheduler

on:
  schedule:
    - cron: '0 0 * * *'  # 매일 UTC 0시 (한국 시간 오전 9시)
    - cron: '8 8 * * *'  # 매일 UTC 8시 8분 (한국 시간 오후 5시 8분)
  workflow_dispatch:  # 수동 실행 가능

jobs:
  run-alerts:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          
      - name: Run analysis
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
        run: |
          python -m telebot.main --run all
```

## 주의사항

- 투자 결정은 본인 책임 하에 이루어져야 합니다.
- 이 봇은 참고용이며, 실제 투자 결정에 대한 책임은 사용자에게 있습니다.
- 주식 시장은 변동성이 크므로 자신의 투자 성향에 맞게 활용하세요. 
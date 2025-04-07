# GitHub 설정 가이드

## 1. GitHub 레포지토리 생성

1. GitHub 계정에 접속합니다.
2. 우측 상단의 "+" 버튼을 클릭하고 "New repository"를 선택합니다.
3. Repository name에 "stock_git"를 입력합니다.
4. 필요에 따라 설명을 추가하고 Privacy 설정(Public/Private)을 선택합니다.
5. "Create repository" 버튼을 클릭합니다.

## 2. 로컬 저장소에서 GitHub로 푸시

레포지토리 생성 후, 아래 명령어를 순서대로 실행합니다:

```bash
# 모든 파일 추가
git add .

# 첫 번째 커밋 생성
git commit -m "Initial commit: 주식 알림 봇 코드 추가"

# 원격 저장소 연결
git remote add origin https://github.com/YOUR_USERNAME/stock_git.git

# 코드 푸시
git push -u origin master
```

## 3. GitHub Secrets 설정

GitHub Actions에서 민감한 정보를 사용하기 위해 레포지토리 시크릿을 설정합니다:

1. GitHub 레포지토리 페이지에서 "Settings" 탭을 클릭합니다.
2. 좌측 메뉴에서 "Secrets and variables" > "Actions"를 선택합니다.
3. "New repository secret" 버튼을 클릭합니다.
4. 아래 시크릿을 추가합니다:
   - `SUPABASE_URL`: Supabase URL
   - `SUPABASE_KEY`: Supabase API 키
   - `TELEGRAM_BOT_TOKEN`: TQQQ 텔레그램 봇 토큰
   - `TELEGRAM_CHAT_ID`: TQQQ 텔레그램 채팅 ID
   - `KOSPI_TELEGRAM_BOT_TOKEN`: 코스피/코스닥 텔레그램 봇 토큰
   - `KOSPI_TELEGRAM_CHAT_ID`: 코스피/코스닥 텔레그램 채팅 ID

## 4. GitHub Actions 확인

1. GitHub 레포지토리 페이지에서 "Actions" 탭을 클릭합니다.
2. "Stock Alert Scheduler" 워크플로우가 표시됩니다.
3. 워크플로우는 다음 시간에 실행됩니다:
   - 매일 오전 9시: TQQQ 알림 (UTC 0:00, 평일만)
   - 매일 오후 5시 8분: 주식 분석 (UTC 8:08, 평일만)
   - 테스트용 오후 9시 10분: 모든 기능 (UTC 12:10)
4. 워크플로우를 즉시 테스트하려면 "Run workflow" 버튼을 클릭합니다.

## 5. Supabase 설정

Supabase에서 다음 작업을 수행합니다:

1. Supabase 프로젝트를 생성합니다.
2. SQL 편집기에서 `schema.sql` 파일의 내용을 실행합니다.
3. 생성된 URL과 API 키를 GitHub Secrets에 추가합니다. 
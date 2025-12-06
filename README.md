# Threads Collector

Threads 프로필의 게시물을 스크래핑하는 API 서버입니다.

## 기능

- Threads 사용자 프로필에서 게시물을 자동으로 수집
- RESTful API를 통한 스크래핑 요청
- 외부 사용자용: 단일 계정 스크래핑 API
- 내부 사용자용: 여러 계정 배치 스크래핑 API

## 설치

### 1. 의존성 설치

```bash
uv sync
```

또는

```bash
pip install -e .
```

### 2. Playwright 브라우저 설치

```bash
playwright install chromium
```

## 사용 방법

### API 서버 실행

```bash
# 방법 1: 간단한 실행 스크립트 (권장)
uv run python run_server.py

# 방법 2: 배치 파일 실행 (Windows)
start_server_simple.bat

# 방법 3: uvicorn 직접 실행
uv run uvicorn app:app --host 0.0.0.0 --port 8001

# 방법 4: Python으로 실행
uv run python app.py
```

**기본 포트: 8001** (포트 충돌 방지를 위해 8001로 변경됨)

서버가 실행되면 다음 주소에서 API 문서를 확인할 수 있습니다:
- Swagger UI: http://localhost:8001/docs
- ReDoc: http://localhost:8001/redoc

### API 엔드포인트

#### 1. 외부 사용자용: GET /scrape

쿼리 파라미터로 사용자명을 받아 단일 계정을 스크래핑합니다.

**예시:**
```bash
curl "http://localhost:8000/scrape?username=zuck&max_posts=10"
```

**파라미터:**
- `username` (필수): Threads 사용자명 (예: "zuck", "@" 기호 없이 입력)
- `max_posts` (선택): 최대 수집할 게시물 수
- `max_scroll_rounds` (선택): 최대 스크롤 라운드 수 (기본값: 50)

#### 2. 외부 사용자용: POST /scrape

JSON body로 사용자명과 옵션을 받아 단일 계정을 스크래핑합니다.

**예시:**
```bash
curl -X POST "http://localhost:8000/scrape" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "zuck",
    "max_posts": 10,
    "max_scroll_rounds": 50
  }'
```

**요청 body:**
```json
{
  "username": "zuck",
  "max_posts": 10,
  "max_scroll_rounds": 50
}
```

#### 3. 내부 사용자용: POST /internal/batch-scrape

계정 리스트를 받아 여러 계정을 한 번에 스크래핑합니다. 계정들은 병렬로 처리됩니다.

**예시:**
```bash
curl -X POST "http://localhost:8000/internal/batch-scrape" \
  -H "Content-Type: application/json" \
  -d '{
    "usernames": ["zuck", "meta", "instagram"],
    "max_posts": 10,
    "max_scroll_rounds": 50
  }'
```

**요청 body:**
```json
{
  "usernames": ["zuck", "meta", "instagram"],
  "max_posts": 10,
  "max_scroll_rounds": 50
}
```

**파라미터:**
- `usernames` (필수): Threads 사용자명 리스트 (최소 1개, 최대 50개)
- `max_posts` (선택): 각 계정당 최대 수집할 게시물 수
- `max_scroll_rounds` (선택): 각 계정당 최대 스크롤 라운드 수 (기본값: 50)

**응답 형식:**
```json
{
  "total_accounts": 3,
  "successful_accounts": 2,
  "failed_accounts": 1,
  "results": [
    {
      "username": "zuck",
      "success": true,
      "total_posts": 10,
      "posts": [...],
      "error": null,
      "scraped_at": "2024-01-01T12:00:00"
    },
    {
      "username": "meta",
      "success": false,
      "total_posts": 0,
      "posts": [],
      "error": "스크래핑 오류 메시지",
      "scraped_at": "2024-01-01T12:00:00"
    }
  ],
  "completed_at": "2024-01-01T12:05:00"
}
```

#### 4. GET /health

서버 상태를 확인합니다.

```bash
curl "http://localhost:8000/health"
```

### 기존 CLI 스크립트 (호환성 유지)

환경변수를 사용하여 직접 실행할 수도 있습니다:

```bash
THREADS_USERNAME_TARGET=zuck python main.py
```

## 응답 형식

### 단일 계정 스크래핑 응답 (GET/POST /scrape)

```json
{
  "username": "zuck",
  "total_posts": 10,
  "posts": [
    {
      "text": "게시물 내용...",
      "created_at": "2024-01-01T00:00:00",
      "url": "https://www.threads.com/@zuck/post/..."
    }
  ],
  "scraped_at": "2024-01-01T12:00:00"
}
```

### 배치 스크래핑 응답 (POST /internal/batch-scrape)

각 계정의 성공/실패 여부와 함께 리스트로 반환됩니다. 일부 계정이 실패해도 다른 계정의 결과는 정상적으로 반환됩니다.

## 배포

### Docker 사용 (권장)

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Playwright 설치
RUN apt-get update && apt-get install -y \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync

RUN playwright install chromium
RUN playwright install-deps chromium

COPY . .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

### 환경 변수

- `PORT`: 서버 포트 (기본값: 8000)
- `HOST`: 서버 호스트 (기본값: 0.0.0.0)

## 주의사항

- Threads 웹 구조가 변경될 수 있어 CSS 셀렉터는 주기적으로 업데이트가 필요할 수 있습니다.
- 스크래핑은 공개 프로필만 지원합니다.
- 과도한 요청은 Threads에서 차단될 수 있으니 적절한 rate limiting을 고려하세요.


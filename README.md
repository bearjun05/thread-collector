# Threads Collector

Threads 프로필의 게시물을 스크래핑하는 API 서버입니다.

## 기능

- Threads 사용자 프로필에서 게시물을 자동으로 수집
- RESTful API를 통한 스크래핑 요청
- 외부 사용자용: 단일 계정 스크래핑 API
- 내부 사용자용: 여러 계정 배치 스크래핑 API
- **날짜 기반 필터링**: 최근 N일 또는 특정 날짜 이후 게시물만 수집
- **게시물 수 제한**: 최대 수집 게시물 수 설정 가능

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
uv run playwright install chromium
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

**기본 포트: 8001**
**브라우저 설치 필수**: `uv run playwright install chromium`

서버가 실행되면 다음 주소에서 API 문서를 확인할 수 있습니다:
- Swagger UI: http://localhost:8001/docs
- ReDoc: http://localhost:8001/redoc

---

## API 엔드포인트

### 1. 외부 사용자용: GET /scrape

쿼리 파라미터로 사용자명을 받아 단일 계정을 스크래핑합니다.

**예시:**
```bash
# 기본 사용
curl "http://localhost:8001/scrape?username=zuck&max_posts=10"

# 최근 7일 게시물만
curl "http://localhost:8001/scrape?username=zuck&since_days=7"

# 최근 30일, 최대 20개
curl "http://localhost:8001/scrape?username=zuck&since_days=30&max_posts=20"

# 특정 날짜 이후
curl "http://localhost:8001/scrape?username=zuck&since_date=2024-12-01"
```

**파라미터:**
| 파라미터 | 필수 | 설명 |
|---------|-----|------|
| `username` | ✅ | Threads 사용자명 (예: "zuck", "@" 기호 없이 입력) |
| `max_posts` | ❌ | 최대 수집할 게시물 수 (미지정시 전체) |
| `since_days` | ❌ | 최근 N일 이내 게시물만 (예: 7=일주일, 30=한달) |
| `since_date` | ❌ | 특정 날짜 이후 게시물만 (ISO 형식, 예: "2024-12-01") |

---

### 2. 외부 사용자용: POST /scrape

JSON body로 사용자명과 옵션을 받아 단일 계정을 스크래핑합니다.

**예시:**
```bash
curl -X POST "http://localhost:8001/scrape" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "zuck",
    "max_posts": 10,
    "since_days": 7
  }'
```

**요청 body:**
```json
{
  "username": "zuck",
  "max_posts": 10,
  "since_days": 7
}
```

---

### 3. 내부 사용자용: POST /internal/batch-scrape

계정 리스트를 받아 여러 계정을 한 번에 스크래핑합니다. 계정들은 병렬로 처리됩니다.

**예시:**
```bash
curl -X POST "http://localhost:8001/internal/batch-scrape" \
  -H "Content-Type: application/json" \
  -d '{
    "usernames": ["zuck", "meta", "instagram"],
    "max_posts": 10,
    "since_days": 7
  }'
```

**요청 body:**
```json
{
  "usernames": ["zuck", "meta", "instagram"],
  "max_posts": 10,
  "max_scroll_rounds": 50,
  "since_days": 7,
  "since_date": null
}
```

**파라미터:**
| 파라미터 | 필수 | 설명 |
|---------|-----|------|
| `usernames` | ✅ | Threads 사용자명 리스트 (최소 1개, 최대 50개) |
| `max_posts` | ❌ | 각 계정당 최대 수집할 게시물 수 |
| `max_scroll_rounds` | ❌ | 각 계정당 최대 스크롤 라운드 수 (기본값: 50) |
| `since_days` | ❌ | 최근 N일 이내 게시물만 |
| `since_date` | ❌ | 특정 날짜 이후 게시물만 (ISO 형식) |

---

### 4. GET /health

서버 상태를 확인합니다.

```bash
curl "http://localhost:8001/health"
```

---

## 응답 형식

### 단일 계정 스크래핑 응답 (GET/POST /scrape)

```json
{
  "username": "zuck",
  "total_posts": 50,
  "filtered_posts": 10,
  "posts": [
    {
      "text": "게시물 내용 (글자수 무제한)...",
      "created_at": "2024-12-01T00:00:00",
      "url": "https://www.threads.com/@zuck/post/..."
    }
  ],
  "scraped_at": "2024-12-06T12:00:00",
  "filter_applied": "최근 7일 이내"
}
```

**응답 필드:**
| 필드 | 설명 |
|-----|------|
| `total_posts` | 스크래핑한 전체 게시물 수 |
| `filtered_posts` | 필터 적용 후 게시물 수 |
| `posts` | 게시물 리스트 |
| `filter_applied` | 적용된 필터 설명 (없으면 null) |

---

### 배치 스크래핑 응답 (POST /internal/batch-scrape)

```json
{
  "total_accounts": 3,
  "successful_accounts": 2,
  "failed_accounts": 1,
  "results": [
    {
      "username": "zuck",
      "success": true,
      "total_posts": 50,
      "filtered_posts": 10,
      "posts": [...],
      "error": null,
      "scraped_at": "2024-12-06T12:00:00",
      "filter_applied": "최근 7일 이내"
    },
    {
      "username": "meta",
      "success": false,
      "total_posts": 0,
      "filtered_posts": 0,
      "posts": [],
      "error": "스크래핑 오류 메시지",
      "scraped_at": "2024-12-06T12:00:00",
      "filter_applied": null
    }
  ],
  "completed_at": "2024-12-06T12:05:00"
}
```

---

## 날짜 필터링 사용 예시

```bash
# 최근 일주일 게시물
curl "http://localhost:8001/scrape?username=zuck&since_days=7"

# 최근 한달 게시물
curl "http://localhost:8001/scrape?username=zuck&since_days=30"

# 특정 날짜 이후
curl "http://localhost:8001/scrape?username=zuck&since_date=2024-11-01"

# 조합: 최근 일주일 + 최대 5개
curl "http://localhost:8001/scrape?username=zuck&since_days=7&max_posts=5"
```

---

## 배포

### Linux 서버 (systemd)

```bash
cd /home/uj/opt/thread-collector
sudo ./setup_systemd.sh
```

### Docker 사용

```bash
docker build -t thread-collector .
docker run -d -p 8001:8001 thread-collector
```

### 환경 변수

- `PORT`: 서버 포트 (기본값: 8001)

---

## 주의사항

- Threads 웹 구조가 변경될 수 있어 CSS 셀렉터는 주기적으로 업데이트가 필요할 수 있습니다.
- 스크래핑은 공개 프로필만 지원합니다.
- 과도한 요청은 Threads에서 차단될 수 있으니 적절한 rate limiting을 고려하세요.
- 게시물 텍스트는 글자수 제한 없이 전체 내용이 반환됩니다.

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
- Admin UI: http://localhost:8001/admin (Basic Auth 필요)

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

### 1-1. 새 응답 형식: POST /v2/scrape

기존 로직은 그대로 두고 **응답 구조만 정리한 새 엔드포인트**입니다.

**예시 (threaded 스타일):**
```bash
curl -X POST "http://localhost:8001/v2/scrape" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "zuck",
    "since_days": 7,
    "include_replies": true,
    "max_reply_depth": 2,
    "max_total_posts": 100,
    "response_style": "threaded"
  }'
```

**예시 (flat 스타일):**
```bash
curl -X POST "http://localhost:8001/v2/scrape" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "zuck",
    "since_days": 7,
    "include_replies": true,
    "max_reply_depth": 2,
    "max_total_posts": 100,
    "response_style": "flat"
  }'
```

**요청 body (주요 필드):**
| 파라미터 | 필수 | 설명 |
|---------|-----|------|
| `username` | ✅ | Threads 사용자명 |
| `since_days` | ❌ | 최근 N일 이내 (기본: 1일) |
| `since_date` | ❌ | 특정 날짜 이후 (since_days보다 우선) |
| `include_replies` | ❌ | 답글 포함 여부 (기본: true) |
| `max_reply_depth` | ❌ | 답글 깊이 (기본: 1) |
| `max_total_posts` | ❌ | 전체 최대 수집 수 (기본: 100) |
| `response_style` | ❌ | `threaded` 또는 `flat` (기본: threaded) |

---

**참고:** `include_replies=false`인 경우 프로필 피드에서 “Replying to / 답글” 라벨이 있는 항목을 제외하여 루트 게시물만 반환합니다.

**응답 구조 (style별 분리):**
- `data.style`: `"threaded"` 또는 `"flat"`
- `data.items`: style에 따른 배열

`threaded`일 때 `data.items`는 스레드 단위 배열 (루트 + replies)  
`flat`일 때 `data.items`는 평탄화된 게시물 배열 (root/reply 구분 + parent_id)

**실제 응답 예시 (2026-02-04 기준, username=choi.openai, threaded, include_replies=false):**
```json
{
  "meta": {
    "username": "choi.openai",
    "scraped_at": "2026-02-04T10:20:24.851211+09:00",
    "duration_seconds": 16.37,
    "scroll_rounds": 11,
    "status": "success"
  },
  "filter": {
    "type": "최근 7일 이내",
    "cutoff_date": "2026-01-28T01:20:08.476865+00:00"
  },
  "stats": {
    "total_scraped": 37,
    "filtered_count": 10,
    "excluded_count": 0,
    "total_replies": 0,
    "posts_with_replies": 0
  },
  "request": {
    "since_days": 7,
    "since_date": null,
    "include_replies": false,
    "max_reply_depth": 1,
    "max_total_posts": 10,
    "response_style": "threaded"
  },
  "data": {
    "style": "threaded",
    "items": [
      {
        "thread_id": null,
        "post": {
          "id": null,
          "url": "https://www.threads.com/@choi.openai/post/DUO6NzIEl2u",
          "text": "와.. 개미쳤습니다 진짜..\\nNC AI에서 VARCO SOUND를 베타 런칭했는데,\\n제가 지금까지 경험했던 어떤 사운드 생성 AI보다도 뛰어납니다.\\n...",
          "created_at": "2026-02-01T23:00:38.000Z",
          "author": null
        },
        "replies": [],
        "counts": {
          "replies": 0,
          "total": 1
        }
      },
      {
        "thread_id": null,
        "post": {
          "id": null,
          "url": "https://www.threads.com/@choi.openai/post/DUO6dgij__p",
          "text": "1/ 먼저, VARCO SOUND는 소리를 한 덩어리로 뽑아주는 방식이 아니라 각 요소를 멀티 트랙... ",
          "created_at": "2026-02-01T23:04:52.000Z",
          "author": null
        },
        "replies": [],
        "counts": {
          "replies": 0,
          "total": 1
        }
      }
    ]
  }
}
```

**실제 응답 예시 (2026-02-04 기준, username=choi.openai, flat, include_replies=false):**
```json
{
  "meta": {
    "username": "choi.openai",
    "scraped_at": "2026-02-04T10:26:34.840454+09:00",
    "duration_seconds": 16.66,
    "scroll_rounds": 11,
    "status": "success"
  },
  "filter": {
    "type": "최근 7일 이내",
    "cutoff_date": "2026-01-28T01:26:18.180026+00:00"
  },
  "stats": {
    "total_scraped": 37,
    "filtered_count": 10,
    "excluded_count": 0,
    "total_replies": 0,
    "posts_with_replies": 0
  },
  "request": {
    "since_days": 7,
    "since_date": null,
    "include_replies": false,
    "max_reply_depth": 1,
    "max_total_posts": 10,
    "response_style": "flat"
  },
  "data": {
    "style": "flat",
    "items": [
      {
        "type": "root",
        "thread_id": null,
        "post": {
          "id": null,
          "url": "https://www.threads.com/@choi.openai/post/DUO6NzIEl2u",
          "text": "와.. 개미쳤습니다 진짜..\\nNC AI에서 VARCO SOUND를 베타 런칭했는데,\\n제가 지금까지 경험했던 어떤 사운드 생성 AI보다도 뛰어납니다.\\n...",
          "created_at": "2026-02-01T23:00:38.000Z",
          "author": null
        },
        "parent_id": null
      },
      {
        "type": "root",
        "thread_id": null,
        "post": {
          "id": null,
          "url": "https://www.threads.com/@choi.openai/post/DUO6dgij__p",
          "text": "1/ 먼저, VARCO SOUND는 소리를 한 덩어리로 뽑아주는 방식이 아니라 각 요소를 멀티 트랙... ",
          "created_at": "2026-02-01T23:04:52.000Z",
          "author": null
        },
        "parent_id": null
      }
    ]
  }
}
```

**실제 응답 예시 (2026-02-04 기준, username=choi.openai, flat, include_replies=true):**
```json
{
  "meta": {
    "username": "choi.openai",
    "scraped_at": "2026-02-04T10:47:32.069239+09:00",
    "duration_seconds": 280.01,
    "scroll_rounds": 11,
    "status": "success"
  },
  "filter": {
    "type": "최근 7일 이내",
    "cutoff_date": "2026-01-28T01:42:52.059455+00:00"
  },
  "stats": {
    "total_scraped": 37,
    "filtered_count": 7,
    "excluded_count": 0,
    "total_replies": 1,
    "posts_with_replies": 1
  },
  "request": {
    "since_days": 7,
    "since_date": null,
    "include_replies": true,
    "max_reply_depth": 1,
    "max_total_posts": 8,
    "response_style": "flat"
  },
  "data": {
    "style": "flat",
    "items": [
      {
        "type": "root",
        "thread_id": "DUT5eiojff5",
        "post": {
          "id": "DUT5eiojff5",
          "url": "https://www.threads.com/@choi.openai/post/DUT5eiojff5",
          "text": "모델의 성능이 정말 가파르게 올라가는 것 같습니다.\\n알리바바 Qwen이 코딩 에이전트와 로컬 개발 환경에 최적화된 오픈 웨이트 모델 \\\"Qwen3-Coder-Next\\\"를 공개했습니다.\\n...",
          "created_at": "2026-02-03T21:30:23.000Z",
          "author": "choi.openai"
        },
        "parent_id": null
      },
      {
        "type": "reply",
        "thread_id": "DUT5eiojff5",
        "post": {
          "id": "DUT5frPDcYG",
          "url": "https://www.threads.com/@choi.openai/post/DUT5frPDcYG",
          "text": "블로그 :\\nqwen.ai/blog…  Translate",
          "created_at": "2026-02-03T21:30:33.000Z",
          "author": "choi.openai"
        },
        "parent_id": "DUT5eiojff5"
      }
    ]
  }
}
```

---

### 1-2. RSS 피드: GET /v2/rss

계정별 RSS 피드를 제공합니다. 토큰 기반 접근입니다.

**예시:**
```bash
curl "http://localhost:8001/v2/rss?username=choi.openai&token=YOUR_TOKEN"
```

**쿼리 파라미터:**
| 파라미터 | 필수 | 설명 |
|---------|-----|------|
| `username` | ✅ | Threads 사용자명 |
| `token` | ✅ | 접근 토큰 |
| `limit` | ❌ | 최대 항목 수 (기본: 50, 최대: 500) |

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

### 3-1. 배치 스크래핑: POST /batch-scrape

여러 계정을 병렬로 처리하는 배치 API입니다. (동시 처리 최대 5개)

**예시:**
```bash
curl -X POST "http://localhost:8001/batch-scrape" \
  -H "Content-Type: application/json" \
  -d '{
    "usernames": ["zuck", "meta", "instagram"],
    "since_days": 7,
    "max_per_account": 10,
    "max_total": 300
  }'
```

---

### 3-2. 새 응답 형식 배치: POST /v2/batch-scrape

기존 배치 로직은 유지하고, **응답 구조만 정리한 v2 배치 엔드포인트**입니다.

**예시:**
```bash
curl -X POST "http://localhost:8001/v2/batch-scrape" \
  -H "Content-Type: application/json" \
  -d '{
    "usernames": ["zuck", "meta", "instagram"],
    "since_days": 7,
    "include_replies": true,
    "max_reply_depth": 2,
    "max_per_account": 10,
    "max_total": 300,
    "response_style": "threaded"
  }'
```

**응답 구조 (요약):**
- `batch_meta`: 전체 배치 메타데이터
- `request`: 배치 요청 파라미터
- `results`: 계정별 결과 배열 (각 요소는 `/v2/scrape`와 동일한 구조)

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

---

## Admin UI

관리자 UI는 `/admin`에서 접근합니다. Basic Auth가 필요합니다.
- `/admin` (Dashboard)
- `/admin/tokens` (Tokens)
- `/admin/settings` (Settings)

환경 변수:
- `ADMIN_USER` (기본: `admin`)
- `ADMIN_PASSWORD` (필수)
 - `ENABLE_INTERNAL_SCHEDULER=1` (내부 스케줄러 사용 시)
 - `ENABLE_SYSTEMD_CONTROL=1` (systemd 제어 허용 시)
 - `SYSTEMD_TIMER_PATH` (systemd timer 파일 경로, 기본: `/etc/systemd/system/thread-collector-rss.timer`)

---

## 스케줄러

### 옵션 A: 외부 스케줄러 (권장)
- `rss_sync.py`를 systemd timer나 cron으로 실행

### 옵션 B: 내부 스케줄러
- `ENABLE_INTERNAL_SCHEDULER=1` 설정 시 API 서버 내부에서 스케줄 실행
- 관리자 UI에서 interval/ON-OFF 조정 가능
- 시작 시간(start_time)은 **KST 입력 → UTC 저장**
- 실행 타이밍은 start_time 기준으로 정렬됨

### systemd 제어 (Admin UI)
- `ENABLE_SYSTEMD_CONTROL=1` 설정 시 `/admin`에서 systemd timer 상태/시작/중지 가능
- 권한이 필요한 환경에서는 `sudo` 권한이 필요할 수 있음
- interval/start_time 변경 시 systemd timer가 자동 업데이트됨

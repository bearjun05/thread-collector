# RSS Token 관리

## 개요
- RSS 엔드포인트는 토큰이 필요합니다.
- 토큰은 Admin UI에서 생성/비활성/삭제합니다.

## 토큰 스코프
- `global`: 모든 계정 RSS 접근 가능
- `username`: 특정 계정만 접근 가능 (예: `choi.openai`)

## 생성 방법 (Admin UI)
1. `/admin` 접속
2. Tokens 섹션에서 scope 입력
3. Create 버튼 클릭 → 토큰 발급

## API로 관리
- 목록: `GET /admin/api/tokens`
- 생성: `POST /admin/api/tokens` (body: `{ "scope": "global" }`)
- 비활성: `PATCH /admin/api/tokens/{id}?is_active=false`
- 삭제: `DELETE /admin/api/tokens/{id}`

## 로그 모니터링
- 목록: `GET /admin/api/token-logs?limit=200`
- 토큰 요청은 `rss_token_logs`에 기록됩니다.
 - 유효하지 않은 토큰은 `rss_invalid_token_logs`에 별도 기록됩니다.

## Rate Limit
- 설정 조회: `GET /admin/api/rate-limit`
- 설정 변경: `PATCH /admin/api/rate-limit`

## Usage Stats
- 집계: `GET /admin/api/token-stats?hours=24`

## CSV Export
- 다운로드: `GET /admin/api/token-logs.csv`

## 사용 예시
```bash
curl "http://localhost:8001/v2/rss?username=choi.openai&token=YOUR_TOKEN"
```

## 스코프 예시
- 글로벌: `scope=global`
- 특정 계정: `scope=choi.openai`

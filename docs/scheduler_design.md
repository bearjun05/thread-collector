# 계정별 스케줄 수집 설계

## 목표
- 등록된 계정별로 주기적으로 Threads 스크랩 수행
- 스크랩 결과를 SQLite에 저장
- RSS 요청은 DB 조회만 수행 (스크랩 없음)

## 기본 구조
1. 계정 목록 소스: `feed_sources` 테이블 (`is_active=1`만 수집)
2. 스케줄러가 계정 목록을 순회
3. 계정별로 스크랩 실행
4. 결과를 DB에 UPSERT(중복 방지)

## 스케줄링 옵션
### A. cron + 스크립트 (가장 단순)
- 리눅스 `cron`에 등록
- 예: 30분마다 실행

### B. systemd timer
- 배포 서버에서 권장
- 실패 시 재시도/로그 관리 용이

### C. 앱 내부 스케줄러
- FastAPI 내부에서 Background Task로 일정 수행
- 프로세스 재시작 시 스케줄 유실 가능성 있음

## 추천 운영 방식
- **systemd timer** + 단일 스크랩 스크립트
- 한 번에 모든 계정을 순회
- 실행 시간 초과 시 다음 배치로 넘어감

## 내부 스케줄러 (옵션)
- `ENABLE_INTERNAL_SCHEDULER=1` 설정 시 API 서버 내부에서 스케줄 실행
- 관리자 UI에서 interval/ON-OFF 조정 가능
- 단일 워커 환경에서만 사용 권장
 - 시작 시간(start_time)은 UTC 기준 HH:MM

## 배치 수집 흐름 (의사 코드)
```
load accounts from feed_sources
for each account:
  scrape latest posts
  for each post:
    insert into posts (post_id UNIQUE)
```

## 동시성 전략
- 계정이 많아지면 동시성 3~5개 제한 권장
- SQLite는 동시 쓰기 약함 → batch 단일 프로세스가 안정적

## 중복 수집 방지
- `posts.post_id`에 UNIQUE 제약
- INSERT 시 충돌되면 무시
  - SQLite: `INSERT OR IGNORE`

## 로그/모니터링
- 배치 시작/종료 로그 남김
- 실패한 계정은 다음 배치에서 재시도

## 향후 확장
- 계정별 스케줄 주기 분리
- 큐레이션 도입 시 curated 테이블 확장

## 구현 예시 (rss_sync.py)
- 동시성 5개 제한
- 스크랩은 병렬
- DB 삽입은 순차 (SQLite 락 최소화)

환경 변수:
- `RSS_DB_PATH`: DB 파일 경로 (기본: db/rss_cache.db)
- `RSS_SCHEMA_PATH`: 스키마 파일 경로 (기본: db/schema.sql)
- `RSS_SCRAPE_CONCURRENCY`: 스크랩 동시성 (기본: 5)
- `RSS_CUTOFF_SAFETY_MINUTES`: 최신 글 기준 안전 여유 (기본: 120)

실행:
```bash
uv run python rss_sync.py
```

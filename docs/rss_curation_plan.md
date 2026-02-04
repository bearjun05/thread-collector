# RSS Curation Plan (Future)

## 목표
- 계정별 RSS 외에 "좋은 퀄리티의 글"을 모아 제공하는 큐레이션 피드 지원

## 접근 방식 (1차 제안)
- 수동 큐레이션
  - 관리자가 특정 post를 큐레이션으로 지정
  - 데이터 수집 로직과 분리하여 운영

## 스키마 확장 후보
### 옵션 A: posts 테이블에 컬럼 추가
- `posts.is_curated` INTEGER(0/1)
- 장점: 단순
- 단점: 큐레이션 이력/메타를 붙이기 어려움

### 옵션 B: 별도 큐레이션 테이블 추가 (추천)
- `curated_posts`
  - `id` PK
  - `post_id` FK (posts.post_id)
  - `curated_at`
  - `curated_by` (optional)
  - `note` (optional)

## 큐레이션 피드 엔드포인트 (예시)
- `/v2/rss?type=curated&token=...`
- `type=curated`는 큐레이션 테이블을 기준으로 조회

## 운영 플로우 (초안)
1. 관리자가 큐레이션할 post를 선택
2. `curated_posts`에 기록
3. RSS 요청 시 curated 목록을 최신 순으로 제공

## 보안/접근 제어
- 토큰 기반 (URL query)
- `tokens.scope=global` 또는 `scope=curated`

## 후속 과제
- 관리자용 큐레이션 API/CLI 추가
- 큐레이션 메타데이터 설계 (카테고리, 태그 등)

"""Threads 프로필 스크래핑 로직 모듈"""
import asyncio
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout, Page, Browser

THREADS_BASE_URL = "https://www.threads.com"

# 타임아웃 설정 (ms)
PAGE_LOAD_TIMEOUT = 30000
NETWORK_IDLE_TIMEOUT = 15000
REPLY_LOAD_TIMEOUT = 10000


def _parse_threads_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Threads time[datetime] 값을 timezone-aware datetime(UTC)로 파싱."""
    if not dt_str:
        return None
    s = dt_str.strip()
    try:
        # 흔한 ISO 포맷: 2024-01-01T00:00:00Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _extract_username(input_str: str) -> str:
    """입력에서 순수한 username만 추출.
    
    다양한 입력 형식 처리:
    - "zuck" → "zuck"
    - "@zuck" → "zuck"
    - "https://www.threads.com/@zuck" → "zuck"
    - "https://www.threads.com/@zuck/post/ABC123" → "zuck"
    - "threads.com/@zuck" → "zuck"
    - "@zuck/post/ABC123" → "zuck"
    """
    input_str = input_str.strip()
    
    # URL에서 username 추출
    url_match = re.search(r'threads\.com/@([^/\s]+)', input_str)
    if url_match:
        return url_match.group(1)
    
    # @username/post/... 형식에서 username만 추출
    at_match = re.match(r'^@?([^/\s@]+)', input_str)
    if at_match:
        return at_match.group(1)
    
    return input_str.lstrip("@")


def _normalize_post_url(url: str) -> str:
    """URL에서 고유한 post ID를 추출하여 정규화.
    
    다양한 URL 형식을 통일:
    - https://www.threads.com/@user/post/ABC123 → ABC123
    - https://www.threads.com/post/ABC123 → ABC123
    - /post/ABC123 → ABC123
    - /@user/post/ABC123 → ABC123
    """
    match = re.search(r'/post/([A-Za-z0-9_-]+)', url)
    if match:
        return match.group(1)
    return url


def _match_score(query: str, username: str, display_name: str) -> int:
    """검색어 매칭 점수 계산 (낮을수록 우선순위 높음)
    
    우선순위:
    0: username이 검색어와 정확히 일치
    1: username이 검색어로 시작
    2: display_name이 검색어로 시작
    3: username에 검색어 포함
    4: display_name에 검색어 포함
    5: 기타
    """
    q = query.lower()
    u = username.lower()
    d = display_name.lower()
    
    if u == q:
        return 0
    if u.startswith(q):
        return 1
    if d.startswith(q):
        return 2
    if q in u:
        return 3
    if q in d:
        return 4
    return 5


async def search_threads_users(
    query: str,
    max_results: int = 10,
) -> List[Dict[str, Any]]:
    """Threads에서 사용자 검색.
    
    Args:
        query: 검색어 (예: "cat", "zuck")
        max_results: 최대 결과 수 (기본값: 10)
    
    Returns:
        사용자 정보 리스트 (검색어 매칭 우선순위로 정렬됨)
    """
    # serp_type=accounts로 사용자 검색 탭 사용
    search_url = f"{THREADS_BASE_URL}/search?q={query}&serp_type=accounts"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            # 더 안정적인 페이지 로딩
            await page.goto(search_url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            try:
                await page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_TIMEOUT)
            except PlaywrightTimeout:
                print(f"[scraper] 검색 페이지 networkidle 타임아웃, 계속 진행...")
            await page.wait_for_timeout(2000)
            
            results: List[Dict[str, Any]] = []
            seen_usernames: set[str] = set()
            
            # 사용자 프로필 링크 찾기
            profile_links = await page.query_selector_all("a[href^='/@']")
            
            for link in profile_links:
                href = await link.get_attribute("href")
                if not href or "/post/" in href:
                    continue
                
                match = re.match(r"^/@([^/]+)$", href)
                if not match:
                    continue
                
                username = match.group(1)
                if username in seen_usernames:
                    continue
                
                display_name = None
                parent = await link.query_selector("xpath=..")
                if parent:
                    text = await parent.inner_text()
                    lines = [l.strip() for l in text.split("\n") if l.strip()]
                    if lines:
                        display_name = lines[0] if lines[0] != f"@{username}" else None
                
                results.append({
                    "username": username,
                    "display_name": display_name or username,
                    "profile_url": f"{THREADS_BASE_URL}/@{username}",
                })
                seen_usernames.add(username)
            
            # 검색 결과가 없거나, 쿼리가 정확한 username처럼 보이면 직접 프로필 확인
            query_clean = _extract_username(query)
            if not any(r["username"].lower() == query_clean.lower() for r in results):
                # 직접 프로필 페이지 확인
                try:
                    profile_url = f"{THREADS_BASE_URL}/@{query_clean}"
                    await page.goto(profile_url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
                    
                    # 프로필이 존재하는지 확인 (404가 아닌 경우)
                    title = await page.title()
                    if "Page not found" not in title and query_clean.lower() not in [r["username"].lower() for r in results]:
                        # display_name 추출 시도
                        name_el = await page.query_selector("h1, [data-testid='user-name']")
                        display_name = await name_el.inner_text() if name_el else query_clean
                        
                        results.insert(0, {
                            "username": query_clean,
                            "display_name": display_name.split("\n")[0].strip() if display_name else query_clean,
                            "profile_url": profile_url,
                        })
                except Exception:
                    pass  # 프로필 확인 실패는 무시
            
            # 검색어 매칭 우선순위로 정렬
            results.sort(key=lambda r: _match_score(query, r["username"], r["display_name"]))
            
            return results[:max_results]
        except Exception as e:
            print(f"검색 오류: {e}")
            return []
        finally:
            await browser.close()


async def scrape_threads_profile(
    username: str,
    max_posts: int | None = None,
    max_scroll_rounds: int = 50,
    cutoff_utc: Optional[datetime] = None,
    cutoff_old_streak_threshold: int = 6,
) -> Dict[str, Any]:
    """주어진 Threads 프로필에서 최신 게시물 일부를 스크래핑하는 함수.
    
    Returns:
        Dict with keys:
        - posts: List[Dict] - 수집된 게시물 리스트
        - scroll_rounds: int - 실제 스크롤 횟수
        - duration_seconds: float - 스크래핑 소요 시간
    """
    import time
    start_time = time.time()
    
    # username 정규화 (URL이나 잘못된 형식 처리)
    clean_username = _extract_username(username)
    print(f"[scraper] 원본 입력: '{username}' → 정규화된 username: '{clean_username}'")
    
    url = f"{THREADS_BASE_URL}/@{clean_username}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 더 안정적인 페이지 로딩: domcontentloaded 먼저, 그 후 추가 대기
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            # 추가로 네트워크가 안정될 때까지 대기 (타임아웃 시 무시)
            try:
                await page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_TIMEOUT)
            except PlaywrightTimeout:
                print(f"[scraper] networkidle 타임아웃, 계속 진행...")
        except PlaywrightTimeout:
            print(f"[scraper] 페이지 로딩 타임아웃: {url}")
            await browser.close()
            return {"posts": [], "scroll_rounds": 0, "duration_seconds": time.time() - start_time}

        post_selector = "div[data-pressable-container='true']"

        results: List[Dict[str, Any]] = []
        seen_post_ids: set[str] = set()
        last_count = 0
        no_new_posts_count = 0  # 연속으로 새 게시물이 없는 횟수
        old_post_streak = 0  # cutoff 이전 게시물이 연속으로 나온 횟수(조기 종료용)
        early_stop = False
        
        # 최소 스크롤 보장 (pinned 게시물 때문에 최신 게시물이 아래에 있을 수 있음)
        MIN_SCROLL_ROUNDS = 5
        MAX_NO_NEW_POSTS = 3  # 연속 3번 새 게시물 없으면 중단
        actual_scroll_rounds = 0

        for scroll_round in range(max_scroll_rounds):
            actual_scroll_rounds = scroll_round + 1
            candidates = await page.query_selector_all(post_selector)

            for node in candidates:
                if max_posts is not None and len(results) >= max_posts:
                    break

                # 컨테이너 안에 '/post/' 링크가 여러 개 있을 수 있어(공유/인용/추천 등)
                # 시간(time)을 포함한 링크를 우선 선택해 URL(post_id)와 텍스트 매칭을 안정화한다.
                post_link = await node.query_selector("a[href*='/post/']:has(time)")
                if not post_link:
                    post_link = await node.query_selector("a[href^='/@'][href*='/post/']")
                if not post_link:
                    post_link = await node.query_selector("a[href*='/post/']")
                if not post_link:
                    continue

                href = await post_link.get_attribute("href")
                if not href:
                    continue

                full_url = f"{THREADS_BASE_URL}{href}" if href.startswith("/") else href
                
                post_id = _normalize_post_url(full_url)
                if post_id in seen_post_ids:
                    continue

                time_el = await node.query_selector("time")
                created_at = await time_el.get_attribute("datetime") if time_el else None

                content_div = await node.query_selector("div.x1a6qonq")
                if content_div:
                    text_content = await content_div.inner_text()
                else:
                    text_content = await node.inner_text()

                results.append({
                    "post_id": post_id,
                    "text": (text_content or "").strip(),
                    "created_at": created_at,
                    "url": full_url,
                })
                seen_post_ids.add(post_id)

                # cutoff 기반 조기 종료 판단 (보수적으로: 최소 스크롤 이후, 연속 N개가 cutoff 이전이면 종료)
                if cutoff_utc is not None:
                    created_dt = _parse_threads_datetime(created_at)
                    if created_dt is not None and created_dt < cutoff_utc:
                        old_post_streak += 1
                    else:
                        old_post_streak = 0

                    if (
                        scroll_round >= MIN_SCROLL_ROUNDS
                        and len(results) >= 10
                        and old_post_streak >= max(1, cutoff_old_streak_threshold)
                    ):
                        print(
                            f"[scraper] cutoff 이전 게시물 {old_post_streak}개 연속 발견 → 조기 종료 "
                            f"(cutoff_utc={cutoff_utc.isoformat()})"
                        )
                        # 내부 루프/외부 루프 모두 종료
                        early_stop = True
                        break
            
            if early_stop:
                break

            if max_posts is not None and len(results) >= max_posts:
                break

            # 스크롤 중단 조건 개선: 최소 스크롤 보장 + 연속 N번 새 게시물 없을 때만 중단
            if len(results) == last_count:
                no_new_posts_count += 1
                # 최소 스크롤 횟수 이후, 연속으로 새 게시물이 없으면 중단
                if scroll_round >= MIN_SCROLL_ROUNDS and no_new_posts_count >= MAX_NO_NEW_POSTS:
                    print(f"[scraper] 스크롤 중단: {scroll_round+1}회 스크롤 후 연속 {no_new_posts_count}번 새 게시물 없음")
                    break
            else:
                no_new_posts_count = 0  # 새 게시물 발견 시 카운터 리셋

            last_count = len(results)

            await page.mouse.wheel(0, 2500)
            await page.wait_for_timeout(1200)

        print(f"[scraper] 프로필 스크래핑 완료: {len(results)}개 게시물 수집 ({actual_scroll_rounds}회 스크롤)")
        await browser.close()

    duration_seconds = time.time() - start_time
    return {
        "posts": results,
        "scroll_rounds": actual_scroll_rounds,
        "duration_seconds": round(duration_seconds, 2),
    }


def _extract_post_id(url: str) -> Optional[str]:
    """URL에서 post ID 추출.
    
    예시:
    - https://www.threads.com/@user/post/ABC123 → ABC123
    - /post/ABC123 → ABC123
    """
    match = re.search(r'/post/([A-Za-z0-9_-]+)', url)
    return match.group(1) if match else None


def _extract_author_from_url(url: str) -> Optional[str]:
    """URL에서 작성자 username 추출.
    
    예시:
    - https://www.threads.com/@user/post/ABC123 → user
    - /@user/post/ABC123 → user
    """
    match = re.search(r'/@([^/]+)/post/', url)
    return match.group(1) if match else None


async def _parse_single_post(node, seen_ids: set) -> Optional[Dict[str, Any]]:
    """단일 게시물 노드에서 데이터 추출."""
    try:
        # 컨테이너 안에 '/post/' 링크가 여러 개 있을 수 있어(공유/인용/추천 등)
        # 시간(time)을 포함한 링크를 우선 선택해 URL(post_id)와 텍스트 매칭을 안정화한다.
        post_link = await node.query_selector("a[href*='/post/']:has(time)")
        if not post_link:
            post_link = await node.query_selector("a[href^='/@'][href*='/post/']")
        if not post_link:
            post_link = await node.query_selector("a[href*='/post/']")
        if not post_link:
            return None
        
        href = await post_link.get_attribute("href")
        if not href:
            return None
        
        full_url = f"{THREADS_BASE_URL}{href}" if href.startswith("/") else href
        post_id = _extract_post_id(full_url)
        
        if not post_id or post_id in seen_ids:
            return None
        
        # 작성자 추출
        author = _extract_author_from_url(full_url)
        
        # 프로필 링크에서 작성자 추출 시도
        if not author:
            author_link = await node.query_selector("a[href^='/@']")
            if author_link:
                author_href = await author_link.get_attribute("href")
                if author_href:
                    author_match = re.match(r'^/@([^/]+)$', author_href)
                    if author_match:
                        author = author_match.group(1)
        
        # 시간 추출
        time_el = await node.query_selector("time")
        created_at = await time_el.get_attribute("datetime") if time_el else None
        
        # 텍스트 내용 추출
        content_div = await node.query_selector("div.x1a6qonq")
        if content_div:
            text_content = await content_div.inner_text()
        else:
            text_content = await node.inner_text()
        
        # 답글 여부 힌트 (프로필 피드에서 root/reply 구분용)
        is_reply = False
        try:
            node_text = await node.inner_text()
            lines = [l.strip() for l in node_text.split("\n") if l.strip()]
            if lines:
                first_line = lines[0]
                if first_line.startswith("Replying to") or first_line.startswith("답글"):
                    is_reply = True
        except Exception:
            is_reply = False

        # 미디어(이미지/영상) 추출
        media = []
        try:
            img_nodes = await node.query_selector_all("img")
            for img in img_nodes:
                src = await img.get_attribute("src") or await img.get_attribute("data-src")
                if not src:
                    continue
                lower_src = src.lower()
                if "profile" in lower_src or "avatar" in lower_src:
                    continue
                try:
                    box = await img.bounding_box()
                    if box and box["width"] < 80 and box["height"] < 80:
                        continue
                except Exception:
                    pass
                media.append({"type": "image", "url": src})

            video_nodes = await node.query_selector_all("video")
            for vid in video_nodes:
                src = await vid.get_attribute("src")
                if not src:
                    source = await vid.query_selector("source")
                    if source:
                        src = await source.get_attribute("src")
                if not src:
                    continue
                media.append({"type": "video", "url": src})
        except Exception:
            media = media or []

        if media:
            seen_media = set()
            deduped = []
            for item in media:
                url = item.get("url")
                if not url or url in seen_media:
                    continue
                seen_media.add(url)
                deduped.append(item)
            media = deduped

        seen_ids.add(post_id)
        
        return {
            "post_id": post_id,
            "text": (text_content or "").strip(),
            "created_at": created_at,
            "url": full_url,
            "author": author,
            "replies": [],  # 답글은 나중에 채워짐
            "is_reply": is_reply,
            "media": media,
        }
    except Exception as e:
        print(f"[scraper] 게시물 파싱 오류: {e}")
        return None


async def _load_more_replies(page: Page, max_clicks: int = 10) -> None:
    """'답글 더 보기' 버튼을 클릭하여 모든 답글 로드."""
    for _ in range(max_clicks):
        try:
            # 다양한 '더 보기' 버튼 패턴 시도
            more_buttons = await page.query_selector_all(
                "div[role='button']:has-text('답글'), "
                "div[role='button']:has-text('replies'), "
                "div[role='button']:has-text('View'), "
                "span:has-text('답글 더 보기'), "
                "span:has-text('View more replies')"
            )
            
            clicked = False
            for btn in more_buttons:
                try:
                    if await btn.is_visible():
                        await btn.click()
                        await page.wait_for_timeout(1500)
                        clicked = True
                        break
                except Exception:
                    continue
            
            if not clicked:
                break
                
        except Exception:
            break


async def _check_recommendation_section(page: Page) -> bool:
    """추천 섹션이 나타났는지 확인."""
    try:
        # 추천/관련 게시물 섹션 감지 패턴
        recommendation_patterns = [
            "text='More threads'",
            "text='다른 스레드'", 
            "text='추천'",
            "text='Recommended'",
            "text='You might like'",
            "text='More from'",
        ]
        
        for pattern in recommendation_patterns:
            el = await page.query_selector(pattern)
            if el and await el.is_visible():
                return True
        return False
    except Exception:
        return False


async def scrape_thread_with_replies(
    post_url: str,
    max_depth: int = 5,
    max_total_posts: int = 100,
) -> Dict[str, Any]:
    """개별 게시물과 모든 답글을 트리 구조로 수집.
    
    Args:
        post_url: 게시물 URL (예: "https://www.threads.com/@user/post/ABC123")
        max_depth: 최대 답글 깊이 (기본값: 5)
        max_total_posts: 최대 수집 게시물 수 - root + 답글 합계 (기본값: 100)
    
    Returns:
        트리 구조의 게시물 데이터:
        {
            "post_id": "ABC123",
            "text": "root post text",
            "created_at": "2024-01-01T00:00:00Z",
            "url": "https://...",
            "author": "username",
            "replies": [
                {
                    "post_id": "DEF456",
                    "text": "reply text",
                    ...
                    "replies": [...]
                }
            ],
            "total_replies_count": 15
        }
    """
    # URL 정규화
    if not post_url.startswith("http"):
        if post_url.startswith("/"):
            post_url = f"{THREADS_BASE_URL}{post_url}"
        else:
            post_url = f"{THREADS_BASE_URL}/{post_url}"
    
    requested_post_id = _extract_post_id(post_url)
    root_author = _extract_author_from_url(post_url)
    
    if not requested_post_id:
        raise ValueError(f"유효하지 않은 게시물 URL: {post_url}")
    
    print(f"[scraper] 스레드 스크래핑 시작: {post_url} (작성자: {root_author})")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            # 페이지 로드
            await page.goto(post_url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            try:
                await page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_TIMEOUT)
            except PlaywrightTimeout:
                print(f"[scraper] networkidle 타임아웃, 계속 진행...")
            
            # 추가 답글 로드
            await _load_more_replies(page)
            
            # 스크롤하여 더 많은 답글 로드 (추천 섹션 발견 시 중단)
            for scroll_round in range(10):  # 최대 10번 스크롤
                # 추천 섹션 체크
                if await _check_recommendation_section(page):
                    print(f"[scraper] 추천 섹션 발견, 스크롤 중단 (round {scroll_round})")
                    break
                
                await page.mouse.wheel(0, 1500)
                await page.wait_for_timeout(800)
                await _load_more_replies(page, max_clicks=2)
            
            # 모든 게시물 컨테이너 수집
            post_selector = "div[data-pressable-container='true']"
            all_nodes = await page.query_selector_all(post_selector)
            
            seen_ids: set = set()
            all_posts: List[Dict[str, Any]] = []
            
            for node in all_nodes:
                post_data = await _parse_single_post(node, seen_ids)
                if not post_data:
                    continue
                
                all_posts.append(post_data)
                
                # 최대 수집 제한
                if len(all_posts) >= max_total_posts:
                    break
            
            print(f"[scraper] 전체 수집된 게시물 수: {len(all_posts)}")
            
            # =============================
            # root(루트) 게시물 정규화
            #
            # Threads에서 "reply URL"로 들어가도 화면 상단에는 보통 상위(부모/루트) 게시물이 함께 렌더링됩니다.
            # 기존 로직은 "요청 URL의 post_id"를 root로 고정해서, 같은 스레드를
            # - 루트 URL로 스크랩 1번
            # - reply URL로 스크랩 1번
            # 처럼 중복 수집하는 문제가 생길 수 있습니다.
            #
            # 따라서 DOM에 나타난 게시물 목록의 "최상단 게시물"을 root로 삼아
            # reply URL로 요청해도 동일한 root post_id로 정규화되도록 합니다.
            # =============================
            if not all_posts:
                raise ValueError(f"게시물을 찾을 수 없습니다: {post_url}")

            # DOM 순서의 첫 게시물을 root로 간주 (가장 상위 게시물)
            root_post = all_posts[0]
            root_index = 0

            # 답글 후보: root 이후에 렌더링된 게시물들
            candidate_posts = all_posts[root_index + 1 :]
            
            # =============================
            # depth 처리
            #
            # 현재 구현은 "루트 + (루트 화면에 표시되는) 답글 목록"을 한 번에 수집합니다.
            # 즉, 기본적으로 root(0) + reply(1) 레벨 수집이며,
            # reply의 reply(2+)를 별도로 재귀 탐색하지 않습니다.
            # =============================
            if max_depth <= 0:
                # 방어적 처리 (API 레이어에서는 ge=1이지만 내부 호출 대비)
                candidate_posts = []

            # =============================
            # 작성자 필터링: 루트 게시물 작성자의 답글(셀프 스레드)만 수집
            #
            # 루트 게시물 작성자와 동일한 작성자의 답글만 포함합니다.
            # 다른 사용자의 답글은 제외됩니다.
            # =============================
            root_author_name = root_post.get("author")
            if root_author_name:
                # 대소문자 무시 비교 + 연속성 필터(다른 작성자 등장 시 이후는 모두 제외)
                root_author_lower = root_author_name.lower()
                filtered = []
                for reply in candidate_posts:
                    author = reply.get("author")
                    if not author or author.lower() != root_author_lower:
                        break
                    filtered.append(reply)
                excluded_count = len(candidate_posts) - len(filtered)
                if excluded_count > 0:
                    print(
                        f"[scraper] 작성자+연속 필터링: {len(candidate_posts)}개 중 {len(filtered)}개만 포함 "
                        f"(다른 작성자/비작성자 {excluded_count}개 제외)"
                    )
                candidate_posts = filtered
            else:
                print(f"[scraper] 경고: 루트 게시물 작성자를 알 수 없어 답글 제외")
                candidate_posts = []

            # 답글을 root에 연결 (루트 작성자의 답글만 포함)
            root_post["replies"] = candidate_posts
            root_post["total_replies_count"] = len(candidate_posts)

            # 요청 URL이 reply였더라도, root 정규화 결과를 로그로 남김
            if requested_post_id and root_post.get("post_id") and requested_post_id != root_post.get("post_id"):
                print(
                    f"[scraper] 요청 post_id({requested_post_id}) → root post_id({root_post.get('post_id')})로 정규화"
                )
            
            return root_post
            
        except Exception as e:
            print(f"[scraper] 스레드 스크래핑 오류: {e}")
            raise
        finally:
            await browser.close()


async def scrape_threads_profile_with_replies(
    username: str,
    max_posts: int | None = None,
    max_scroll_rounds: int = 50,
    include_replies: bool = False,
    max_reply_depth: int = 3,
    max_total_posts: int = 100,
    cutoff_utc: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """프로필의 게시물을 수집하고, 선택적으로 각 게시물의 답글도 수집.
    
    Args:
        username: Threads 사용자명
        max_posts: 최대 수집할 게시물 수
        max_scroll_rounds: 최대 스크롤 횟수
        include_replies: True면 각 게시물의 답글도 수집 (시간 오래 걸림)
        max_reply_depth: 답글 수집 시 최대 깊이
        max_total_posts: 전체 출력 최대 게시물 수 (모든 root + 모든 답글 합계, 기본값: 100)
    
    Returns:
        게시물 리스트 (include_replies=True면 각 게시물에 replies 필드 포함)
    """
    # 먼저 프로필에서 root 게시물들 수집
    scrape_result = await scrape_threads_profile(
        username=username,
        max_posts=max_posts,
        max_scroll_rounds=max_scroll_rounds,
        cutoff_utc=cutoff_utc,
    )
    posts = scrape_result["posts"]
    
    if not include_replies:
        return posts[:max_total_posts] if max_total_posts else posts
    
    # 각 게시물의 답글 수집 (전체 게시물 수 추적)
    print(f"[scraper] {len(posts)}개 게시물의 답글 수집 시작... (전체 최대 {max_total_posts}개)")
    
    results_with_replies = []
    total_collected = 0  # 전체 수집된 게시물 수 (root + 답글)
    
    for i, post in enumerate(posts):
        # 이미 최대치에 도달했으면 중단
        if total_collected >= max_total_posts:
            print(f"[scraper] 전체 최대 게시물 수({max_total_posts})에 도달, 수집 중단")
            break
        
        print(f"[scraper] 답글 수집 중: {i+1}/{len(posts)} - {post.get('url', 'unknown')} (현재 총 {total_collected}개)")
        
        try:
            # 남은 수집 가능 개수 계산
            remaining = max_total_posts - total_collected
            
            thread_data = await scrape_thread_with_replies(
                post_url=post["url"],
                max_depth=max_reply_depth,
                max_total_posts=remaining,  # 남은 개수만큼만 수집
            )
            
            # 수집된 게시물 수 계산 (root 1개 + 답글 수)
            replies_count = len(thread_data.get("replies", []))
            thread_total = 1 + replies_count
            total_collected += thread_total
            
            results_with_replies.append(thread_data)
            print(f"[scraper] 스레드 수집 완료: root 1개 + 답글 {replies_count}개 = {thread_total}개 (누적 {total_collected}개)")
            
        except Exception as e:
            print(f"[scraper] 답글 수집 실패: {e}")
            # 답글 수집 실패 시 원본 게시물만 추가
            post["replies"] = []
            post["total_replies_count"] = 0
            results_with_replies.append(post)
            total_collected += 1  # root만 카운트
        
        # 요청 간 간격 (rate limiting 방지)
        await asyncio.sleep(2)
    
    print(f"[scraper] 전체 수집 완료: 총 {total_collected}개 게시물")
    return results_with_replies

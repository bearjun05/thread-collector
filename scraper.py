"""Threads 프로필 스크래핑 로직 모듈"""
import asyncio
import re
from typing import List, Dict, Any, Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout, Page, Browser

THREADS_BASE_URL = "https://www.threads.com"

# 타임아웃 설정 (ms)
PAGE_LOAD_TIMEOUT = 30000
NETWORK_IDLE_TIMEOUT = 15000
REPLY_LOAD_TIMEOUT = 10000


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
) -> List[Dict[str, Any]]:
    """주어진 Threads 프로필에서 최신 게시물 일부를 스크래핑하는 함수."""
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
            return []

        post_selector = "div[data-pressable-container='true']"

        results: List[Dict[str, Any]] = []
        seen_post_ids: set[str] = set()
        last_count = 0

        for scroll_round in range(max_scroll_rounds):
            candidates = await page.query_selector_all(post_selector)

            for node in candidates:
                if max_posts is not None and len(results) >= max_posts:
                    break

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
                    "text": (text_content or "").strip(),
                    "created_at": created_at,
                    "url": full_url,
                })
                seen_post_ids.add(post_id)

            if max_posts is not None and len(results) >= max_posts:
                break

            if len(results) == last_count:
                break

            last_count = len(results)

            await page.mouse.wheel(0, 2500)
            await page.wait_for_timeout(1200)

        await browser.close()

    return results


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
        
        seen_ids.add(post_id)
        
        return {
            "post_id": post_id,
            "text": (text_content or "").strip(),
            "created_at": created_at,
            "url": full_url,
            "author": author,
            "replies": [],  # 답글은 나중에 채워짐
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
    
    post_id = _extract_post_id(post_url)
    root_author = _extract_author_from_url(post_url)
    
    if not post_id:
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
            found_root = False
            
            for node in all_nodes:
                post_data = await _parse_single_post(node, seen_ids)
                if not post_data:
                    continue
                
                # root 게시물 발견 체크
                if post_data["post_id"] == post_id:
                    found_root = True
                
                all_posts.append(post_data)
                
                # 최대 수집 제한
                if len(all_posts) >= max_total_posts:
                    break
            
            print(f"[scraper] 전체 수집된 게시물 수: {len(all_posts)}")
            
            # root 게시물 찾기
            root_post = None
            replies = []
            
            for post in all_posts:
                if post["post_id"] == post_id:
                    root_post = post
                else:
                    replies.append(post)
            
            # root가 없으면 첫 번째 게시물을 root로 사용
            if not root_post and all_posts:
                root_post = all_posts[0]
                replies = all_posts[1:]
            
            if not root_post:
                raise ValueError(f"게시물을 찾을 수 없습니다: {post_url}")
            
            # 답글 필터링: 원작자가 작성한 답글만 남기기
            # - root 게시물 작성자와 동일한 작성자의 답글만 수집
            # - 다른 사용자의 답글(추천/관련 게시물 포함)은 제외
            filtered_replies = []
            root_found_in_list = False
            root_author_name = root_post.get("author") if root_post else root_author
            
            print(f"[scraper] 원작자: {root_author_name}")
            
            for post in all_posts:
                if post["post_id"] == post_id:
                    root_found_in_list = True
                    continue
                
                # root를 찾은 후의 게시물만 답글로 취급
                if root_found_in_list:
                    post_author = post.get("author")
                    
                    # 원작자가 작성한 답글만 포함
                    if post_author and root_author_name and post_author.lower() == root_author_name.lower():
                        filtered_replies.append(post)
                        print(f"[scraper] ✅ 원작자 답글 포함: {post.get('post_id')} by {post_author}")
                    else:
                        print(f"[scraper] ❌ 다른 사용자 답글 제외: {post.get('post_id')} by {post_author}")
            
            print(f"[scraper] 필터링 후 답글 수: {len(filtered_replies)} (원작자: {root_author_name})")
            
            # 답글을 root에 연결
            root_post["replies"] = filtered_replies
            root_post["total_replies_count"] = len(filtered_replies)
            
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
    posts = await scrape_threads_profile(
        username=username,
        max_posts=max_posts,
        max_scroll_rounds=max_scroll_rounds,
    )
    
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

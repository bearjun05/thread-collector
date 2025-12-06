"""Threads 프로필 스크래핑 로직 모듈"""
import asyncio
import re
from typing import List, Dict, Any

from playwright.async_api import async_playwright

THREADS_BASE_URL = "https://www.threads.com"


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
    search_url = f"{THREADS_BASE_URL}/search?q={query}&serp_type=default"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            await page.goto(search_url, wait_until="networkidle", timeout=15000)
            await page.wait_for_timeout(2000)
            
            results: List[Dict[str, Any]] = []
            seen_usernames: set[str] = set()
            
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
    url = f"{THREADS_BASE_URL}/@{username}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(url, wait_until="networkidle")

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

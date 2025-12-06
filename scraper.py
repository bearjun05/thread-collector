"""Threads 프로필 스크래핑 로직 모듈"""
import asyncio
from typing import List, Dict, Any

from playwright.async_api import async_playwright

THREADS_BASE_URL = "https://www.threads.com"


async def scrape_threads_profile(
    username: str,
    max_posts: int | None = None,
    max_scroll_rounds: int = 50,
) -> List[Dict[str, Any]]:
    """주어진 Threads 프로필에서 최신 게시물 일부를 스크래핑하는 함수.

    - 로그인 없이 공개 프로필 기준으로 동작하도록 시도합니다.
    - Threads 웹 구조가 자주 바뀔 수 있기 때문에, CSS 셀렉터는 필요 시 수정이 필요합니다.
    """
    url = f"{THREADS_BASE_URL}/@{username}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(url, wait_until="networkidle")

        # Threads 웹에서는 <article> 대신 여러 div 조합으로 포스트를 렌더링합니다.
        # 각 포스트 컨테이너는 data-pressable-container="true" 를 가지고 있고,
        # 그 안에 "/@.../post/..." 형태의 링크가 들어 있습니다.
        post_selector = "div[data-pressable-container='true']"

        results: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        last_count = 0

        for scroll_round in range(max_scroll_rounds):
            candidates = await page.query_selector_all(post_selector)

            for node in candidates:
                # 상한이 정해져 있으면 도달 시 바로 종료
                if max_posts is not None and len(results) >= max_posts:
                    break

                # 실제 Threads 포스트인지 확인: /@user/post/... 링크와 time 태그가 있는지 검사
                post_link = await node.query_selector("a[href*='/post/']")
                if not post_link:
                    continue

                href = await post_link.get_attribute("href")
                if not href:
                    continue

                full_url = f"{THREADS_BASE_URL}{href}" if href.startswith("/") else href

                # 이미 본 URL 이면 스킵 (위로 다시 스크롤될 수 있음)
                if full_url in seen_urls:
                    continue

                time_el = await node.query_selector("time")
                created_at = await time_el.get_attribute("datetime") if time_el else None

                # 본문 텍스트 추출: 내용이 들어있는 div 를 우선 시도, 없으면 전체 노드 텍스트 사용
                content_div = await node.query_selector("div.x1a6qonq")  # 스크린샷 기준 본문이 들어 있던 컨테이너
                if content_div:
                    text_content = await content_div.inner_text()
                else:
                    text_content = await node.inner_text()

                results.append(
                    {
                        "text": (text_content or "").strip(),
                        "created_at": created_at,
                        "url": full_url,
                    }
                )
                seen_urls.add(full_url)

            # 상한에 도달했으면 루프 종료
            if max_posts is not None and len(results) >= max_posts:
                break

            # 직전 라운드 이후로 새 게시물이 하나도 안 생겼으면 종료
            if len(results) == last_count:
                break

            last_count = len(results)

            # 다음 배치를 로딩하기 위해 아래로 스크롤
            await page.mouse.wheel(0, 2500)
            await page.wait_for_timeout(1200)

        await browser.close()

    return results


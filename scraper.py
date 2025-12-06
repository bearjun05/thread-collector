"""Threads 프로필 스크래핑 로직 모듈"""
import asyncio
import json
from typing import List, Dict, Any

from playwright.async_api import async_playwright
from pathlib import Path

THREADS_BASE_URL = "https://www.threads.com"

# 스크랩 안정성을 위한 설정 (우선순위 프로파일: 엄격 → 완화)
SELECTOR_PROFILES = [
    {
        "post_containers": [
            "div[data-pressable-container='true']",
            "div:has(a[href*='/post/'])",
            "div[role='article']",
        ],
        "content_candidates": [
            "div.x1a6qonq",  # 관찰된 본문 컨테이너
            "div:has(time)",
            "div",
        ],
        "link_selector": "a[href*='/post/']",
        "time_selector": "time[datetime]",
        "score_threshold": 2,
    },
    # 완화 프로파일: 컨테이너를 줄이고 링크 중심으로
    {
        "post_containers": [
            "div:has(a[href*='/post/'])",
            "div[role='article']",
            "div",  # 최후 수단
        ],
        "content_candidates": [
            "div:has(time)",
            "div",
        ],
        "link_selector": "a[href*='/post/']",
        "time_selector": "time",
        "score_threshold": 1,
    },
]

SCROLL_DISTANCE = 4000
SCROLL_WAIT_MS = 2000
SECOND_CHANCE_WAIT_MS = 1500
MAX_SCROLL_ROUNDS_DEFAULT = 100
MIN_EXPECTED_POSTS = 1  # 0 또는 이 값 미만이면 디버그 덤프

def _flatten_graphql_posts(payload: Any, username: str) -> List[Dict[str, Any]]:
    """GraphQL JSON 응답에서 게시물 후보를 휴리스틱으로 추출."""
    posts: List[Dict[str, Any]] = []

    def walk(node: Any):
        if isinstance(node, dict):
            text = node.get("text") or node.get("caption") or ""
            candidate_id = node.get("id") or node.get("post_id") or node.get("pk") or node.get("code")
            url = node.get("url")
            created_at = node.get("created_at") or node.get("taken_at")
            if isinstance(text, str) and len(text.strip()) > 0 and (candidate_id or url):
                full_url = url
                if not full_url and candidate_id:
                    full_url = f"{THREADS_BASE_URL}/@{username}/post/{candidate_id}"
                posts.append(
                    {
                        "text": text.strip(),
                        "created_at": created_at,
                        "url": full_url or "",
                    }
                )
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return posts


async def scrape_threads_profile(
    username: str,
    max_posts: int | None = None,
    max_scroll_rounds: int = MAX_SCROLL_ROUNDS_DEFAULT,
) -> List[Dict[str, Any]]:
    """주어진 Threads 프로필에서 최신 게시물 일부를 스크래핑하는 함수.

    - 로그인 없이 공개 프로필 기준으로 동작하도록 시도합니다.
    - Threads 웹 구조가 자주 바뀔 수 있기 때문에, 여러 셀렉터를 순차로 시도합니다.
    """
    url = f"{THREADS_BASE_URL}/@{username}"

    async def first_non_empty_nodes(sel_list: list[str]) -> list:
        """셀렉터 우선순위대로 탐색하여 최초로 발견된 노드 리스트 반환."""
        for sel in sel_list:
            nodes = await page.query_selector_all(sel)
            if nodes:
                return nodes
        return []

    async def extract_text(node, content_candidates: list[str]) -> str:
        for sel in content_candidates:
            target = await node.query_selector(sel)
            if target:
                txt = await target.inner_text()
                if txt:
                    return txt
        txt = await node.inner_text()
        return txt or ""

    async def score_node(node, cfg) -> int:
        score = 0
        if await node.query_selector(cfg["link_selector"]):
            score += 2
        if await node.query_selector(cfg["time_selector"]):
            score += 1
        text = await extract_text(node, cfg["content_candidates"])
        if len(text.strip()) > 0:
            score += 1
        return score

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 네트워크 기반 파싱을 위한 훅
        network_posts: List[Dict[str, Any]] = []
        graphql_bodies: List[Dict[str, Any]] = []

        async def on_response(response):
            try:
                url_resp = response.url
                if "graphql" not in url_resp:
                    return
                if response.status != 200:
                    return
                ctype = response.headers.get("content-type", "")
                if "application/json" not in ctype and "application/x-www-form-urlencoded" not in ctype:
                    return
                body_text = await response.text()
                if not body_text:
                    return
                try:
                    payload = json.loads(body_text)
                except json.JSONDecodeError:
                    return
                posts = _flatten_graphql_posts(payload, username)
                if posts:
                    network_posts.extend(posts)
                if len(graphql_bodies) < 3:
                    graphql_bodies.append(payload)
            except Exception:
                return

        page.on("response", on_response)

        await page.set_viewport_size({"width": 1280, "height": 2000})
        await page.goto(url, wait_until="networkidle")
        results: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()

        for profile_idx, cfg in enumerate(SELECTOR_PROFILES, start=1):
            last_count = 0
            try:
                await page.wait_for_selector(cfg["post_containers"][0], timeout=5000)
            except Exception:
                pass

            for scroll_round in range(max_scroll_rounds):
                candidates = await first_non_empty_nodes(cfg["post_containers"])
                scored = []
                for node in candidates:
                    sc = await score_node(node, cfg)
                    if sc >= cfg.get("score_threshold", 1):
                        scored.append((sc, node))
                scored.sort(key=lambda x: x[0], reverse=True)
                candidates = [n for _, n in scored] if scored else candidates
                new_found = False

                for node in candidates:
                    if max_posts is not None and len(results) >= max_posts:
                        break

                    post_link = await node.query_selector(cfg["link_selector"])
                    if not post_link:
                        continue

                    href = await post_link.get_attribute("href")
                    if not href:
                        continue

                    full_url = f"{THREADS_BASE_URL}{href}" if href.startswith("/") else href
                    if full_url in seen_urls:
                        continue

                    time_el = await node.query_selector(cfg["time_selector"])
                    created_at = await time_el.get_attribute("datetime") if time_el else None

                    text_content = await extract_text(node, cfg["content_candidates"])

                    results.append(
                        {
                            "text": text_content.strip(),
                            "created_at": created_at,
                            "url": full_url,
                        }
                    )
                    seen_urls.add(full_url)
                    new_found = True

                if max_posts is not None and len(results) >= max_posts:
                    break

                if not new_found:
                    await page.wait_for_timeout(SECOND_CHANCE_WAIT_MS)
                    if len(results) == last_count:
                        break

                last_count = len(results)
                await page.mouse.wheel(0, SCROLL_DISTANCE)
                await page.wait_for_timeout(SCROLL_WAIT_MS)

            # 현재 프로파일에서 결과가 있으면 종료
            if len(results) >= MIN_EXPECTED_POSTS:
                break

        # 네트워크 기반 보강: DOM 수집이 부족하면 네트워크 추출을 병합
        if len(results) < MIN_EXPECTED_POSTS and network_posts:
            for post in network_posts:
                url = post.get("url") or ""
                if url and url in seen_urls:
                    continue
                results.append(post)
                if url:
                    seen_urls.add(url)

        # 수집 실패 시 디버그 덤프 남기기
        if len(results) < MIN_EXPECTED_POSTS:
            from datetime import datetime
            debug_dir = Path("debug")
            debug_dir.mkdir(exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            html_path = debug_dir / f"threads_{username}_{ts}.html"
            screenshot_path = debug_dir / f"threads_{username}_{ts}.png"
            html_path.write_text(await page.content(), encoding="utf-8")
            await page.screenshot(path=screenshot_path, full_page=True)
            # 네트워크 JSON도 일부 덤프
            if graphql_bodies:
                json_path = debug_dir / f"threads_{username}_{ts}.network.json"
                json_path.write_text(json.dumps(graphql_bodies, ensure_ascii=False, indent=2), encoding="utf-8")
                print(
                    f"[DEBUG] Few or no posts found (count={len(results)}). "
                    f"Saved HTML to {html_path}, screenshot to {screenshot_path}, network JSON to {json_path}"
                )
            else:
                print(
                    f"[DEBUG] Few or no posts found (count={len(results)}). "
                    f"Saved HTML to {html_path}, screenshot to {screenshot_path}"
                )

        await browser.close()

    return results


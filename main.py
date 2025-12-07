"""기존 CLI 스크립트 (호환성 유지)"""
import asyncio
import os
from datetime import datetime
from pathlib import Path

from scraper import scrape_threads_profile


async def main() -> None:
    """간단한 테스트용 엔트리포인트.

    - 환경변수 THREADS_USERNAME_TARGET 에서 스크래핑할 프로필 이름을 읽습니다.
      (예: 'zuck' 처럼 @를 뺀 유저명)
    """
    username = os.getenv("THREADS_USERNAME_TARGET", "choi.openai")

    print(f"Threads 프로필 @{username} 에서 게시물 가져오는 중...")
    # max_posts 를 None 으로 두면 스크롤이 멈출 때까지 가능한 한 많이 가져옵니다.
    posts = await scrape_threads_profile(username=username, max_posts=None)

    print(f"\n가져온 게시물 수: {len(posts)}\n")
    for i, post in enumerate(posts, start=1):
        print(f"[{i}] ------------------------------")
        print(post["text"][:500])  # 너무 길 경우 500자만 미리보기
        print()

    # 결과를 문서 파일(Markdown)로 저장
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)

    file_path = output_dir / f"{username}_threads_{timestamp}.md"

    lines: list[str] = []
    lines.append(f"# Threads posts for @{username} ({len(posts)} items)")
    lines.append("")

    for idx, post in enumerate(posts, start=1):
        created_at = post.get("created_at") or "unknown"
        url = post.get("url") or ""
        text = post.get("text") or ""

        lines.append(f"## {idx}. {created_at}")
        if url:
            lines.append(f"- URL: {url}")
        lines.append("")
        lines.append(text)
        lines.append("")
        lines.append("---")
        lines.append("")

    file_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[INFO] 결과를 파일로 저장했습니다: {file_path}")


if __name__ == "__main__":
    asyncio.run(main())


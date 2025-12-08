"""간단한 디버깅 스크립트"""
import asyncio
from datetime import datetime, timezone, timedelta
from dateutil import parser as date_parser

# 결과를 파일에 직접 저장
OUTPUT_FILE = "g:/내 드라이브/vibe-project/thread-collector/debug_output.txt"

def log(msg):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

async def main():
    # 파일 초기화
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("=== 디버깅 시작 ===\n")
    
    from scraper import scrape_threads_profile
    
    KST = timezone(timedelta(hours=9))
    UTC = timezone.utc
    
    log(f"현재 시간 KST: {datetime.now(KST)}")
    log(f"현재 시간 UTC: {datetime.now(UTC)}")
    log("")
    
    # 스크래핑 실행
    log("scrape_threads_profile 호출 중...")
    posts = await scrape_threads_profile('choi.openai', max_posts=20)
    log(f"수집된 게시물: {len(posts)}개")
    log("")
    
    # 각 게시물 정보 출력
    for i, post in enumerate(posts):
        url = post.get("url", "")
        post_id = url.split("/post/")[-1] if "/post/" in url else "unknown"
        created_at = post.get("created_at")
        log(f"[{i+1}] {post_id}")
        log(f"    created_at: {created_at}")
        log(f"    text: {post.get('text', '')[:50]}...")
    
    # 날짜 필터링 테스트
    log("")
    log("=== 날짜 필터링 테스트 ===")
    
    now_kst = datetime.now(KST)
    cutoff_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff_utc = cutoff_kst.astimezone(UTC)
    
    log(f"since_days=1 cutoff: {cutoff_utc} (UTC)")
    
    passed = 0
    for post in posts:
        created_at_str = post.get("created_at")
        if created_at_str:
            try:
                dt = date_parser.parse(created_at_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                if dt >= cutoff_utc:
                    passed += 1
            except:
                passed += 1  # 파싱 실패 시 포함
        else:
            passed += 1  # 날짜 없으면 포함
    
    log(f"필터 통과: {passed}/{len(posts)}")
    log("")
    log("=== 디버깅 완료 ===")

if __name__ == "__main__":
    asyncio.run(main())

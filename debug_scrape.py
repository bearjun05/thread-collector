"""스크래핑 디버깅 스크립트 - 단계별로 문제 파악"""
import asyncio
from datetime import datetime, timezone, timedelta
from dateutil import parser as date_parser
from scraper import scrape_threads_profile

# 한국 표준 시간대
KST = timezone(timedelta(hours=9))
UTC = timezone.utc

def parse_datetime(dt_str):
    """날짜 문자열 파싱"""
    if not dt_str:
        return None
    try:
        return date_parser.parse(dt_str)
    except:
        return None

def make_aware(dt):
    """timezone-aware로 변환"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt

async def debug_scrape():
    print("=" * 70)
    print("🔍 choi.openai 스크래핑 디버깅")
    print("=" * 70)
    
    # 1단계: 프로필에서 게시물 수집
    print("\n📌 1단계: scrape_threads_profile 호출")
    print("-" * 70)
    
    posts = await scrape_threads_profile('choi.openai', max_posts=20)
    
    print(f"\n✅ 수집된 게시물: {len(posts)}개")
    
    if len(posts) == 0:
        print("❌ 문제: 게시물이 전혀 수집되지 않음!")
        print("   → scrape_threads_profile에서 문제 발생")
        return
    
    # 2단계: 각 게시물의 created_at 확인
    print("\n📌 2단계: created_at 필드 확인")
    print("-" * 70)
    
    none_count = 0
    valid_count = 0
    
    for i, post in enumerate(posts):
        created_at = post.get("created_at")
        url = post.get("url", "")
        post_id = url.split("/post/")[-1] if "/post/" in url else "unknown"
        
        if created_at is None:
            none_count += 1
            print(f"[{i+1}] {post_id}: created_at = None ❌")
        else:
            valid_count += 1
            print(f"[{i+1}] {post_id}: created_at = {created_at} ✅")
    
    print(f"\n✅ created_at 있음: {valid_count}개")
    print(f"❌ created_at 없음: {none_count}개")
    
    if none_count > 0:
        print(f"\n⚠️ 주의: {none_count}개 게시물에 날짜 정보 없음")
    
    # 3단계: 날짜 필터링 시뮬레이션
    print("\n📌 3단계: 날짜 필터링 시뮬레이션")
    print("-" * 70)
    
    now_kst = datetime.now(KST)
    now_utc = datetime.now(UTC)
    
    # since_days=1 일 때 cutoff 계산
    since_days = 1
    cutoff_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=since_days - 1)
    cutoff_utc = cutoff_kst.astimezone(UTC)
    
    print(f"현재 KST: {now_kst.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"현재 UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"since_days=1 cutoff KST: {cutoff_kst.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"since_days=1 cutoff UTC: {cutoff_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    filtered_posts = []
    
    for i, post in enumerate(posts):
        created_at_str = post.get("created_at")
        url = post.get("url", "")
        post_id = url.split("/post/")[-1] if "/post/" in url else "unknown"
        
        if created_at_str is None:
            # 날짜 정보 없으면 포함 (보수적 처리)
            filtered_posts.append(post)
            print(f"[{i+1}] {post_id}: 날짜 없음 → 포함 (보수적)")
        else:
            created_at = parse_datetime(created_at_str)
            if created_at is None:
                filtered_posts.append(post)
                print(f"[{i+1}] {post_id}: 파싱 실패 → 포함 (보수적)")
            else:
                created_at_aware = make_aware(created_at)
                created_at_kst = created_at_aware.astimezone(KST)
                
                if created_at_aware >= cutoff_utc:
                    filtered_posts.append(post)
                    status = "✅ 포함"
                else:
                    status = "❌ 제외"
                
                print(f"[{i+1}] {post_id}: {created_at_kst.strftime('%Y-%m-%d %H:%M')} KST → {status}")
    
    print(f"\n📊 결과: {len(posts)}개 중 {len(filtered_posts)}개 통과 (since_days=1)")
    
    # 4단계: since_days=7 테스트
    print("\n📌 4단계: since_days=7 테스트")
    print("-" * 70)
    
    cutoff_7days_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7 - 1)
    cutoff_7days_utc = cutoff_7days_kst.astimezone(UTC)
    
    print(f"since_days=7 cutoff UTC: {cutoff_7days_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    
    filtered_7days = []
    for post in posts:
        created_at_str = post.get("created_at")
        if created_at_str is None:
            filtered_7days.append(post)
        else:
            created_at = parse_datetime(created_at_str)
            if created_at:
                created_at_aware = make_aware(created_at)
                if created_at_aware >= cutoff_7days_utc:
                    filtered_7days.append(post)
            else:
                filtered_7days.append(post)
    
    print(f"since_days=7 결과: {len(posts)}개 중 {len(filtered_7days)}개 통과")
    
    # 최종 요약
    print("\n" + "=" * 70)
    print("📋 최종 요약")
    print("=" * 70)
    print(f"1. 수집된 전체 게시물: {len(posts)}개")
    print(f"2. created_at 있는 게시물: {valid_count}개")
    print(f"3. created_at 없는 게시물: {none_count}개")
    print(f"4. since_days=1 필터 통과: {len(filtered_posts)}개")
    print(f"5. since_days=7 필터 통과: {len(filtered_7days)}개")
    
    if len(filtered_posts) == 0 and len(posts) > 0:
        print("\n🚨 문제 발견: 게시물은 수집되지만 1일 필터에서 모두 제외됨")
        print("   → created_at 값이 실제로 오래된 것이거나, 필터링 로직 문제")
    
    if none_count == len(posts):
        print("\n🚨 문제 발견: 모든 게시물의 created_at이 None")
        print("   → time 엘리먼트 셀렉터 문제 가능성")

if __name__ == "__main__":
    import sys
    # 결과를 파일로도 저장
    class Tee:
        def __init__(self, *files):
            self.files = files
        def write(self, obj):
            for f in self.files:
                f.write(obj)
                f.flush()
        def flush(self):
            for f in self.files:
                f.flush()
    
    log_file = open("debug_result.txt", "w", encoding="utf-8")
    sys.stdout = Tee(sys.stdout, log_file)
    
    asyncio.run(debug_scrape())
    
    log_file.close()

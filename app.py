"""Threads 스크래퍼 API 서버"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from dateutil import parser as date_parser

from scraper import scrape_threads_profile

app = FastAPI(
    title="Threads Collector API",
    description="Threads 프로필 게시물을 스크래핑하는 API 서버",
    version="0.1.0",
)


def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """다양한 형식의 날짜 문자열을 datetime으로 파싱."""
    if not dt_str:
        return None
    try:
        return date_parser.parse(dt_str)
    except Exception:
        return None


def make_aware(dt: datetime) -> datetime:
    """datetime을 timezone-aware로 변환 (UTC 기준)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def filter_posts_by_date(
    posts: List[Dict[str, Any]],
    since_days: Optional[int] = None,
    since_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """게시물을 날짜 기준으로 필터링.
    
    Args:
        posts: 게시물 리스트
        since_days: 최근 N일 이내 게시물만 (예: 7 = 일주일)
        since_date: 특정 날짜 이후 게시물만 (ISO 형식, 예: "2024-01-01")
    """
    if not since_days and not since_date:
        return posts
    
    # 기준 날짜 계산 (UTC 기준)
    if since_days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    elif since_date:
        parsed = parse_datetime(since_date)
        if not parsed:
            return posts  # 파싱 실패 시 필터링 없이 반환
        cutoff = make_aware(parsed)
    else:
        return posts
    
    filtered = []
    for post in posts:
        created_at = parse_datetime(post.get("created_at"))
        if created_at is None:
            # 날짜 정보 없으면 포함 (보수적 처리)
            filtered.append(post)
        else:
            # 양쪽 모두 timezone-aware로 변환 후 비교
            created_at_aware = make_aware(created_at)
            if created_at_aware >= cutoff:
                filtered.append(post)
    
    return filtered


class ScrapeRequest(BaseModel):
    """스크래핑 요청 모델 (외부 사용자용)"""
    username: str = Field(..., description="Threads 사용자명 (예: 'zuck', '@' 없이 입력)", json_schema_extra={"example": "zuck"})
    max_posts: Optional[int] = Field(None, description="최대 수집할 게시물 수 (미지정시 전체)", json_schema_extra={"example": 10})
    since_days: Optional[int] = Field(None, description="최근 N일 이내 게시물만 (예: 7=일주일, 30=한달)", ge=1, le=365, json_schema_extra={"example": 7})
    since_date: Optional[str] = Field(None, description="특정 날짜 이후 게시물만 (ISO 형식, 예: '2024-12-01')", json_schema_extra={"example": "2024-12-01"})
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "username": "zuck",
                    "max_posts": 10,
                    "since_days": 7
                }
            ]
        }
    }


class PostResponse(BaseModel):
    """게시물 응답 모델"""
    text: str
    created_at: Optional[str]
    url: str


class ScrapeResponse(BaseModel):
    """스크래핑 응답 모델"""
    username: str
    total_posts: int
    filtered_posts: int
    posts: List[PostResponse]
    scraped_at: str
    filter_applied: Optional[str] = None


class BatchScrapeRequest(BaseModel):
    """배치 스크래핑 요청 모델 (내부 사용자용)"""
    usernames: List[str] = Field(..., description="Threads 사용자명 리스트 (예: ['zuck', 'meta'])", min_length=1, max_length=50)
    max_posts: Optional[int] = Field(None, description="각 계정당 최대 수집할 게시물 수 (None이면 제한 없음)")
    max_scroll_rounds: int = Field(50, description="각 계정당 최대 스크롤 라운드 수", ge=1, le=200)
    since_days: Optional[int] = Field(None, description="최근 N일 이내 게시물만", ge=1, le=365)
    since_date: Optional[str] = Field(None, description="특정 날짜 이후 게시물만 (ISO 형식)")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "usernames": ["zuck", "meta", "instagram"],
                    "max_posts": 10,
                    "since_days": 7
                }
            ]
        }
    }


class BatchScrapeItem(BaseModel):
    """배치 스크래핑 결과 항목"""
    username: str
    success: bool
    total_posts: int
    filtered_posts: int
    posts: List[PostResponse]
    error: Optional[str] = None
    scraped_at: str
    filter_applied: Optional[str] = None


class BatchScrapeResponse(BaseModel):
    """배치 스크래핑 응답 모델"""
    total_accounts: int
    successful_accounts: int
    failed_accounts: int
    results: List[BatchScrapeItem]
    completed_at: str


@app.get("/")
async def root():
    """API 루트 엔드포인트"""
    return {
        "message": "Threads Collector API",
        "version": "0.1.0",
        "endpoints": {
            "GET /scrape": "외부 사용자용 - 쿼리 파라미터로 사용자명을 받아 스크래핑",
            "POST /scrape": "외부 사용자용 - JSON body로 사용자명과 옵션을 받아 스크래핑",
            "POST /internal/batch-scrape": "내부 사용자용 - 계정 리스트를 받아 배치 스크래핑",
            "GET /health": "서버 상태 확인",
        },
        "date_filter_options": {
            "since_days": "최근 N일 이내 게시물만 (예: 7=일주일, 30=한달)",
            "since_date": "특정 날짜 이후 게시물만 (ISO 형식, 예: '2024-01-01')",
        },
    }


@app.get("/health")
async def health():
    """서버 상태 확인"""
    return {"status": "healthy"}


@app.get("/scrape", response_model=ScrapeResponse)
async def scrape_get(
    username: str = Query(..., description="Threads 사용자명 (예: 'zuck')"),
    max_posts: Optional[int] = Query(None, description="최대 수집할 게시물 수 (미지정시 전체)"),
    since_days: Optional[int] = Query(None, description="최근 N일 이내 게시물만 (예: 7=일주일, 30=한달)", ge=1, le=365),
    since_date: Optional[str] = Query(None, description="특정 날짜 이후 게시물만 (ISO 형식, 예: '2024-12-01')"),
):
    """GET 방식으로 스크래핑 요청 (외부 사용자용)
    
    예시: 
    - GET /scrape?username=zuck&max_posts=10
    - GET /scrape?username=zuck&since_days=7  (최근 일주일)
    - GET /scrape?username=zuck&since_date=2024-01-01
    """
    try:
        username = username.lstrip("@")
        
        if not username:
            raise HTTPException(status_code=400, detail="사용자명이 필요합니다")
        
        posts = await scrape_threads_profile(
            username=username,
            max_posts=max_posts,
            max_scroll_rounds=200,  # 내부적으로 충분히 높은 값 사용
        )
        
        total_before_filter = len(posts)
        
        # 날짜 필터 적용
        filtered_posts = filter_posts_by_date(posts, since_days=since_days, since_date=since_date)
        
        # 필터 설명 생성
        filter_desc = None
        if since_days:
            filter_desc = f"최근 {since_days}일 이내"
        elif since_date:
            filter_desc = f"{since_date} 이후"
        
        return ScrapeResponse(
            username=username,
            total_posts=total_before_filter,
            filtered_posts=len(filtered_posts),
            posts=[PostResponse(**post) for post in filtered_posts],
            scraped_at=datetime.now().isoformat(),
            filter_applied=filter_desc,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스크래핑 중 오류 발생: {str(e)}")


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape_post(request: ScrapeRequest):
    """POST 방식으로 스크래핑 요청 (외부 사용자용)
    
    예시:
    ```json
    {
        "username": "zuck",
        "max_posts": 10,
        "since_days": 7
    }
    ```
    """
    try:
        username = request.username.lstrip("@")
        
        if not username:
            raise HTTPException(status_code=400, detail="사용자명이 필요합니다")
        
        posts = await scrape_threads_profile(
            username=username,
            max_posts=request.max_posts,
            max_scroll_rounds=200,  # 내부적으로 충분히 높은 값 사용
        )
        
        total_before_filter = len(posts)
        
        # 날짜 필터 적용
        filtered_posts = filter_posts_by_date(posts, since_days=request.since_days, since_date=request.since_date)
        
        # 필터 설명 생성
        filter_desc = None
        if request.since_days:
            filter_desc = f"최근 {request.since_days}일 이내"
        elif request.since_date:
            filter_desc = f"{request.since_date} 이후"
        
        return ScrapeResponse(
            username=username,
            total_posts=total_before_filter,
            filtered_posts=len(filtered_posts),
            posts=[PostResponse(**post) for post in filtered_posts],
            scraped_at=datetime.now().isoformat(),
            filter_applied=filter_desc,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스크래핑 중 오류 발생: {str(e)}")


async def scrape_single_account(
    username: str,
    max_posts: Optional[int],
    max_scroll_rounds: int,
    since_days: Optional[int] = None,
    since_date: Optional[str] = None,
) -> BatchScrapeItem:
    """단일 계정 스크래핑 헬퍼 함수"""
    username = username.lstrip("@")
    scraped_at = datetime.now().isoformat()
    
    try:
        if not username:
            return BatchScrapeItem(
                username=username,
                success=False,
                total_posts=0,
                filtered_posts=0,
                posts=[],
                error="사용자명이 비어있습니다",
                scraped_at=scraped_at,
            )
        
        posts = await scrape_threads_profile(
            username=username,
            max_posts=max_posts,
            max_scroll_rounds=max_scroll_rounds,
        )
        
        total_before_filter = len(posts)
        
        # 날짜 필터 적용
        filtered_posts = filter_posts_by_date(posts, since_days=since_days, since_date=since_date)
        
        # 필터 설명 생성
        filter_desc = None
        if since_days:
            filter_desc = f"최근 {since_days}일 이내"
        elif since_date:
            filter_desc = f"{since_date} 이후"
        
        return BatchScrapeItem(
            username=username,
            success=True,
            total_posts=total_before_filter,
            filtered_posts=len(filtered_posts),
            posts=[PostResponse(**post) for post in filtered_posts],
            error=None,
            scraped_at=scraped_at,
            filter_applied=filter_desc,
        )
    except Exception as e:
        return BatchScrapeItem(
            username=username,
            success=False,
            total_posts=0,
            filtered_posts=0,
            posts=[],
            error=str(e),
            scraped_at=scraped_at,
        )


@app.post("/internal/batch-scrape", response_model=BatchScrapeResponse)
async def batch_scrape(request: BatchScrapeRequest):
    """배치 스크래핑 요청 (내부 사용자용)
    
    여러 계정을 한 번에 스크래핑하여 리스트로 반환합니다.
    계정들은 병렬로 처리됩니다.
    
    예시:
    ```json
    {
        "usernames": ["zuck", "meta", "instagram"],
        "max_posts": 10,
        "since_days": 7
    }
    ```
    """
    try:
        if not request.usernames:
            raise HTTPException(status_code=400, detail="사용자명 리스트가 비어있습니다")
        
        unique_usernames = list(dict.fromkeys(request.usernames))
        
        tasks = [
            scrape_single_account(
                username=username,
                max_posts=request.max_posts,
                max_scroll_rounds=request.max_scroll_rounds,
                since_days=request.since_days,
                since_date=request.since_date,
            )
            for username in unique_usernames
        ]
        
        results = await asyncio.gather(*tasks)
        
        successful_count = sum(1 for r in results if r.success)
        failed_count = len(results) - successful_count
        
        return BatchScrapeResponse(
            total_accounts=len(results),
            successful_accounts=successful_count,
            failed_accounts=failed_count,
            results=results,
            completed_at=datetime.now().isoformat(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"배치 스크래핑 중 오류 발생: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.getenv("PORT", "8001"))
    print(f"서버 시작 중... http://0.0.0.0:{port}")
    print(f"API 문서: http://localhost:{port}/docs")
    uvicorn.run(app, host="0.0.0.0", port=port)

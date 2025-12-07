"""Threads 스크래퍼 API 서버"""
import asyncio
import traceback
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from dateutil import parser as date_parser

from scraper import (
    scrape_threads_profile,
    search_threads_users,
    scrape_thread_with_replies,
    scrape_threads_profile_with_replies,
)

# 한국 표준 시간대 (KST = UTC+9)
KST = timezone(timedelta(hours=9))
UTC = timezone.utc

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
    """게시물을 날짜 기준으로 필터링 (한국 시간 KST 기준).
    
    Args:
        posts: 게시물 리스트
        since_days: 최근 N일 이내 게시물만 (예: 7 = 일주일)
        since_date: 특정 날짜 이후 게시물만 (ISO 형식, 예: "2024-01-01")
    """
    if not since_days and not since_date:
        return posts
    
    # 기준 날짜 계산 (한국 시간 KST 기준)
    now_kst = datetime.now(KST)
    
    if since_days:
        # 한국 시간 기준으로 N일 전 자정부터
        cutoff_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=since_days - 1)
        cutoff = cutoff_kst.astimezone(UTC)  # UTC로 변환하여 비교
    elif since_date:
        parsed = parse_datetime(since_date)
        if not parsed:
            return posts  # 파싱 실패 시 필터링 없이 반환
        # 입력된 날짜를 한국 시간으로 해석
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=KST)
        cutoff = parsed.astimezone(UTC)
    else:
        return posts
    
    filtered = []
    for post in posts:
        created_at = parse_datetime(post.get("created_at"))
        if created_at is None:
            # 날짜 정보 없으면 포함 (보수적 처리)
            filtered.append(post)
        else:
            # UTC로 변환하여 비교
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


class UserSearchResult(BaseModel):
    """사용자 검색 결과 항목"""
    username: str
    display_name: str
    profile_url: str


class UserSearchResponse(BaseModel):
    """사용자 검색 응답 모델"""
    query: str
    total_results: int
    users: List[UserSearchResult]


# ============ 답글 포함 스크래핑 모델 ============

class ReplyPost(BaseModel):
    """답글 게시물 모델 (재귀 구조)"""
    post_id: Optional[str] = None
    text: str
    created_at: Optional[str] = None
    url: str
    author: Optional[str] = None
    replies: List["ReplyPost"] = []

# Pydantic v2 forward reference 해결
ReplyPost.model_rebuild()


class ThreadWithRepliesResponse(BaseModel):
    """답글 포함 스레드 응답 모델"""
    post_id: Optional[str] = None
    text: str
    created_at: Optional[str] = None
    url: str
    author: Optional[str] = None
    replies: List[ReplyPost] = []
    total_replies_count: int = 0
    scraped_at: str


class ScrapeThreadRequest(BaseModel):
    """개별 스레드 스크래핑 요청 모델"""
    post_url: str = Field(..., description="게시물 URL (예: 'https://www.threads.com/@user/post/ABC123')")
    max_depth: int = Field(5, description="최대 답글 깊이", ge=1, le=10)
    max_replies_per_level: int = Field(100, description="각 레벨당 최대 답글 수", ge=1, le=500)
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "post_url": "https://www.threads.com/@zuck/post/ABC123",
                    "max_depth": 5,
                    "max_replies_per_level": 100
                }
            ]
        }
    }


class ScrapeWithRepliesRequest(BaseModel):
    """답글 포함 프로필 스크래핑 요청 모델"""
    username: str = Field(..., description="Threads 사용자명")
    max_posts: Optional[int] = Field(None, description="최대 수집할 게시물 수")
    include_replies: bool = Field(True, description="답글 포함 여부")
    max_reply_depth: int = Field(3, description="최대 답글 깊이", ge=1, le=10)
    since_days: Optional[int] = Field(None, description="최근 N일 이내 게시물만", ge=1, le=365)
    since_date: Optional[str] = Field(None, description="특정 날짜 이후 게시물만 (ISO 형식)")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "username": "zuck",
                    "max_posts": 5,
                    "include_replies": True,
                    "max_reply_depth": 3
                }
            ]
        }
    }


class ScrapeWithRepliesResponse(BaseModel):
    """답글 포함 스크래핑 응답 모델"""
    username: str
    total_posts: int
    posts: List[ThreadWithRepliesResponse]
    scraped_at: str
    filter_applied: Optional[str] = None


@app.get("/")
async def root():
    """API 루트 엔드포인트"""
    return {
        "message": "Threads Collector API",
        "version": "0.3.0",
        "endpoints": {
            "GET /scrape": "⭐ 메인 API - 사용자명만 입력하면 스레드 + 모든 답글 수집 (기본: 최근 1일)",
            "GET /search-users": "사용자 검색 (자동완성) - 검색어로 Threads 사용자 찾기",
            "GET /scrape-thread": "개별 게시물과 모든 답글 수집 (URL로 요청)",
            "POST /scrape-thread": "개별 게시물과 모든 답글 수집 (JSON body)",
            "POST /scrape-with-replies": "프로필 게시물 + 각 게시물의 답글 수집",
            "POST /internal/batch-scrape": "내부 사용자용 - 계정 리스트를 받아 배치 스크래핑",
            "GET /health": "서버 상태 확인",
        },
        "quick_start": {
            "example": "GET /scrape?username=zuck",
            "description": "사용자명만 입력하면 최근 1일 동안의 모든 게시물과 답글을 자동 수집",
        },
        "options": {
            "since_days": "기간 설정 (기본: 1일, 예: since_days=7 → 일주일)",
            "include_replies": "답글 포함 여부 (기본: true)",
            "max_reply_depth": "답글 깊이 (기본: 5)",
        },
    }


@app.get("/health")
async def health():
    """서버 상태 확인"""
    return {"status": "healthy"}


# ============ 답글 포함 스크래핑 엔드포인트 ============

@app.get("/scrape-thread", response_model=ThreadWithRepliesResponse)
async def scrape_thread_get(
    url: str = Query(..., description="게시물 URL (예: 'https://www.threads.com/@user/post/ABC123')"),
    max_depth: int = Query(5, description="최대 답글 깊이", ge=1, le=10),
    max_replies: int = Query(100, description="각 레벨당 최대 답글 수", ge=1, le=500),
):
    """GET 방식으로 개별 게시물과 모든 답글 수집
    
    예시:
    - GET /scrape-thread?url=https://www.threads.com/@zuck/post/ABC123
    - GET /scrape-thread?url=https://www.threads.com/@zuck/post/ABC123&max_depth=3
    """
    try:
        thread_data = await scrape_thread_with_replies(
            post_url=url,
            max_depth=max_depth,
            max_replies_per_level=max_replies,
        )
        
        # ReplyPost 변환
        def convert_replies(replies: List[Dict]) -> List[ReplyPost]:
            result = []
            for r in replies:
                result.append(ReplyPost(
                    post_id=r.get("post_id"),
                    text=r.get("text", ""),
                    created_at=r.get("created_at"),
                    url=r.get("url", ""),
                    author=r.get("author"),
                    replies=convert_replies(r.get("replies", [])),
                ))
            return result
        
        return ThreadWithRepliesResponse(
            post_id=thread_data.get("post_id"),
            text=thread_data.get("text", ""),
            created_at=thread_data.get("created_at"),
            url=thread_data.get("url", ""),
            author=thread_data.get("author"),
            replies=convert_replies(thread_data.get("replies", [])),
            total_replies_count=thread_data.get("total_replies_count", 0),
            scraped_at=datetime.now(KST).isoformat(),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"스레드 스크래핑 오류:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"스레드 스크래핑 중 오류 발생: {str(e)}")


@app.post("/scrape-thread", response_model=ThreadWithRepliesResponse)
async def scrape_thread_post(request: ScrapeThreadRequest):
    """POST 방식으로 개별 게시물과 모든 답글 수집
    
    예시:
    ```json
    {
        "post_url": "https://www.threads.com/@zuck/post/ABC123",
        "max_depth": 5,
        "max_replies_per_level": 100
    }
    ```
    """
    try:
        thread_data = await scrape_thread_with_replies(
            post_url=request.post_url,
            max_depth=request.max_depth,
            max_replies_per_level=request.max_replies_per_level,
        )
        
        # ReplyPost 변환
        def convert_replies(replies: List[Dict]) -> List[ReplyPost]:
            result = []
            for r in replies:
                result.append(ReplyPost(
                    post_id=r.get("post_id"),
                    text=r.get("text", ""),
                    created_at=r.get("created_at"),
                    url=r.get("url", ""),
                    author=r.get("author"),
                    replies=convert_replies(r.get("replies", [])),
                ))
            return result
        
        return ThreadWithRepliesResponse(
            post_id=thread_data.get("post_id"),
            text=thread_data.get("text", ""),
            created_at=thread_data.get("created_at"),
            url=thread_data.get("url", ""),
            author=thread_data.get("author"),
            replies=convert_replies(thread_data.get("replies", [])),
            total_replies_count=thread_data.get("total_replies_count", 0),
            scraped_at=datetime.now(KST).isoformat(),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"스레드 스크래핑 오류:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"스레드 스크래핑 중 오류 발생: {str(e)}")


@app.post("/scrape-with-replies", response_model=ScrapeWithRepliesResponse)
async def scrape_profile_with_replies(request: ScrapeWithRepliesRequest):
    """프로필의 게시물과 각 게시물의 답글을 모두 수집
    
    ⚠️ 주의: 답글 수집 시 각 게시물마다 페이지 방문이 필요하므로 시간이 오래 걸릴 수 있습니다.
    
    예시:
    ```json
    {
        "username": "zuck",
        "max_posts": 5,
        "include_replies": true,
        "max_reply_depth": 3
    }
    ```
    """
    try:
        username = request.username.lstrip("@")
        
        if not username:
            raise HTTPException(status_code=400, detail="사용자명이 필요합니다")
        
        posts = await scrape_threads_profile_with_replies(
            username=username,
            max_posts=request.max_posts,
            include_replies=request.include_replies,
            max_reply_depth=request.max_reply_depth,
        )
        
        # 날짜 필터 적용
        if request.since_days or request.since_date:
            posts = filter_posts_by_date(posts, since_days=request.since_days, since_date=request.since_date)
        
        # 필터 설명 생성
        filter_desc = None
        if request.since_days:
            filter_desc = f"최근 {request.since_days}일 이내"
        elif request.since_date:
            filter_desc = f"{request.since_date} 이후"
        
        # ReplyPost 변환 함수
        def convert_replies(replies: List[Dict]) -> List[ReplyPost]:
            result = []
            for r in replies:
                result.append(ReplyPost(
                    post_id=r.get("post_id"),
                    text=r.get("text", ""),
                    created_at=r.get("created_at"),
                    url=r.get("url", ""),
                    author=r.get("author"),
                    replies=convert_replies(r.get("replies", [])),
                ))
            return result
        
        # ThreadWithRepliesResponse 변환
        thread_responses = []
        for post in posts:
            thread_responses.append(ThreadWithRepliesResponse(
                post_id=post.get("post_id"),
                text=post.get("text", ""),
                created_at=post.get("created_at"),
                url=post.get("url", ""),
                author=post.get("author"),
                replies=convert_replies(post.get("replies", [])),
                total_replies_count=post.get("total_replies_count", 0),
                scraped_at=datetime.now(KST).isoformat(),
            ))
        
        return ScrapeWithRepliesResponse(
            username=username,
            total_posts=len(thread_responses),
            posts=thread_responses,
            scraped_at=datetime.now(KST).isoformat(),
            filter_applied=filter_desc,
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"프로필+답글 스크래핑 오류:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"스크래핑 중 오류 발생: {str(e)}")


@app.get("/search-users", response_model=UserSearchResponse)
async def search_users(
    q: str = Query(..., description="검색어 (예: 'cat', 'zuck')", min_length=1),
    max_results: int = Query(10, description="최대 결과 수", ge=1, le=50),
):
    """Threads 사용자 검색 (자동완성용)
    
    검색어를 입력하면 매칭되는 Threads 사용자 목록을 반환합니다.
    
    예시:
    - GET /search-users?q=cat  → cheese.cat.ai 등 검색
    - GET /search-users?q=zuck&max_results=5
    """
    try:
        users = await search_threads_users(query=q, max_results=max_results)
        
        return UserSearchResponse(
            query=q,
            total_results=len(users),
            users=[UserSearchResult(**user) for user in users],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"사용자 검색 중 오류 발생: {str(e)}")


@app.get("/scrape")
async def scrape_get(
    username: str = Query(..., description="Threads 사용자명 (예: 'zuck')"),
    include_replies: bool = Query(True, description="답글 포함 여부 (기본: True)"),
    since_days: int = Query(1, description="최근 N일 이내 게시물만 (기본: 1일)", ge=1, le=365),
    since_date: Optional[str] = Query(None, description="특정 날짜 이후 게시물만 (ISO 형식, since_days보다 우선)"),
    max_reply_depth: int = Query(5, description="답글 수집 시 최대 깊이", ge=1, le=10),
    max_total_posts: int = Query(100, description="전체 출력 최대 게시물 수 (모든 root + 모든 답글 합계, 기본: 100)", ge=1, le=1000),
):
    """사용자명으로 스레드 + 모든 답글 수집
    
    기본값:
    - 최근 1일 이내 게시물
    - 답글 포함
    - 전체 출력 최대 100개 게시물
    
    예시: 
    - GET /scrape?username=zuck  (최근 1일, 답글 포함, 총 100개)
    - GET /scrape?username=zuck&since_days=7  (최근 일주일)
    - GET /scrape?username=zuck&max_total_posts=50  (총 50개로 제한)
    """
    try:
        username = username.lstrip("@")
        
        if not username:
            raise HTTPException(status_code=400, detail="사용자명이 필요합니다")
        
        # 답글 포함 여부에 따라 다른 함수 사용
        if include_replies:
            posts = await scrape_threads_profile_with_replies(
                username=username,
                max_posts=None,  # 프로필 게시물은 제한 없음
                include_replies=True,
                max_reply_depth=max_reply_depth,
                max_total_posts=max_total_posts,
            )
        else:
            posts = await scrape_threads_profile(
                username=username,
                max_posts=None,  # 제한 없음
                max_scroll_rounds=200,
            )
        
        total_before_filter = len(posts)
        
        # 날짜 필터 적용 (since_date가 있으면 우선)
        if since_date:
            filtered_posts = filter_posts_by_date(posts, since_date=since_date)
            filter_desc = f"{since_date} 이후"
        else:
            filtered_posts = filter_posts_by_date(posts, since_days=since_days)
            filter_desc = f"최근 {since_days}일 이내"
        
        # 답글 포함 시 ThreadWithRepliesResponse 반환
        if include_replies:
            def convert_replies(replies: List[Dict]) -> List[ReplyPost]:
                result = []
                for r in replies:
                    result.append(ReplyPost(
                        post_id=r.get("post_id"),
                        text=r.get("text", ""),
                        created_at=r.get("created_at"),
                        url=r.get("url", ""),
                        author=r.get("author"),
                        replies=convert_replies(r.get("replies", [])),
                    ))
                return result
            
            thread_responses = []
            for post in filtered_posts:
                thread_responses.append(ThreadWithRepliesResponse(
                    post_id=post.get("post_id"),
                    text=post.get("text", ""),
                    created_at=post.get("created_at"),
                    url=post.get("url", ""),
                    author=post.get("author"),
                    replies=convert_replies(post.get("replies", [])),
                    total_replies_count=post.get("total_replies_count", 0),
                    scraped_at=datetime.now(KST).isoformat(),
                ))
            
            return ScrapeWithRepliesResponse(
                username=username,
                total_posts=len(thread_responses),
                posts=thread_responses,
                scraped_at=datetime.now(KST).isoformat(),
                filter_applied=filter_desc,
            )
        else:
            # 답글 미포함 시 기존 ScrapeResponse 반환
            post_responses = []
            for post in filtered_posts:
                try:
                    post_data = {
                        "text": post.get("text") or "",
                        "created_at": post.get("created_at"),
                        "url": post.get("url") or "",
                    }
                    post_responses.append(PostResponse(**post_data))
                except Exception as post_error:
                    print(f"PostResponse 변환 실패: {post_error}")
                    continue
            
            return ScrapeResponse(
                username=username,
                total_posts=total_before_filter,
                filtered_posts=len(post_responses),
                posts=post_responses,
                scraped_at=datetime.now(KST).isoformat(),
                filter_applied=filter_desc,
            )
    except HTTPException:
        raise
    except Exception as e:
        error_detail = f"스크래핑 중 오류 발생: {str(e)}"
        print(f"스크래핑 오류 상세:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_detail)


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
        
        # PostResponse 생성 시 안전하게 처리
        post_responses = []
        for post in filtered_posts:
            try:
                # 필수 필드 검증 및 기본값 설정
                post_data = {
                    "text": post.get("text") or "",
                    "created_at": post.get("created_at"),
                    "url": post.get("url") or "",
                }
                post_responses.append(PostResponse(**post_data))
            except Exception as post_error:
                # 개별 post 변환 실패는 로그만 남기고 건너뜀
                print(f"PostResponse 변환 실패: {post_error}")
                print(f"Post 데이터: {post}")
                print(traceback.format_exc())
                continue
        
        return ScrapeResponse(
            username=username,
            total_posts=total_before_filter,
            filtered_posts=len(post_responses),
            posts=post_responses,
            scraped_at=datetime.now(KST).isoformat(),
            filter_applied=filter_desc,
        )
    except HTTPException:
        raise
    except Exception as e:
        error_detail = f"스크래핑 중 오류 발생: {str(e)}"
        print(f"스크래핑 오류 상세:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_detail)


async def scrape_single_account(
    username: str,
    max_posts: Optional[int],
    max_scroll_rounds: int,
    since_days: Optional[int] = None,
    since_date: Optional[str] = None,
) -> BatchScrapeItem:
    """단일 계정 스크래핑 헬퍼 함수"""
    username = username.lstrip("@")
    scraped_at = datetime.now(KST).isoformat()
    
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
        
        # PostResponse 생성 시 안전하게 처리
        post_responses = []
        for post in filtered_posts:
            try:
                # 필수 필드 검증 및 기본값 설정
                post_data = {
                    "text": post.get("text") or "",
                    "created_at": post.get("created_at"),
                    "url": post.get("url") or "",
                }
                post_responses.append(PostResponse(**post_data))
            except Exception as post_error:
                # 개별 post 변환 실패는 로그만 남기고 건너뜀
                print(f"PostResponse 변환 실패: {post_error}")
                print(f"Post 데이터: {post}")
                print(traceback.format_exc())
                continue
        
        return BatchScrapeItem(
            username=username,
            success=True,
            total_posts=total_before_filter,
            filtered_posts=len(post_responses),
            posts=post_responses,
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
            completed_at=datetime.now(KST).isoformat(),
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

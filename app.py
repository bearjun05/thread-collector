"""Threads 스크래퍼 API 서버"""
import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from scraper import scrape_threads_profile

app = FastAPI(
    title="Threads Collector API",
    description="Threads 프로필 게시물을 스크래핑하는 API 서버",
    version="0.1.0",
)


class ScrapeRequest(BaseModel):
    """스크래핑 요청 모델"""
    username: str = Field(..., description="Threads 사용자명 (예: 'zuck', '@' 없이 입력)")
    max_posts: Optional[int] = Field(None, description="최대 수집할 게시물 수 (None이면 제한 없음)")
    max_scroll_rounds: int = Field(50, description="최대 스크롤 라운드 수", ge=1, le=200)


class PostResponse(BaseModel):
    """게시물 응답 모델"""
    text: str
    created_at: Optional[str]
    url: str


class ScrapeResponse(BaseModel):
    """스크래핑 응답 모델"""
    username: str
    total_posts: int
    posts: List[PostResponse]
    scraped_at: str


class BatchScrapeRequest(BaseModel):
    """배치 스크래핑 요청 모델 (내부 사용자용)"""
    usernames: List[str] = Field(..., description="Threads 사용자명 리스트 (예: ['zuck', 'meta'])", min_length=1, max_length=50)
    max_posts: Optional[int] = Field(None, description="각 계정당 최대 수집할 게시물 수 (None이면 제한 없음)")
    max_scroll_rounds: int = Field(50, description="각 계정당 최대 스크롤 라운드 수", ge=1, le=200)


class BatchScrapeItem(BaseModel):
    """배치 스크래핑 결과 항목"""
    username: str
    success: bool
    total_posts: int
    posts: List[PostResponse]
    error: Optional[str] = None
    scraped_at: str


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
    }


@app.get("/health")
async def health():
    """서버 상태 확인"""
    return {"status": "healthy"}


@app.get("/scrape", response_model=ScrapeResponse)
async def scrape_get(
    username: str = Query(..., description="Threads 사용자명 (예: 'zuck')"),
    max_posts: Optional[int] = Query(None, description="최대 수집할 게시물 수"),
    max_scroll_rounds: int = Query(50, description="최대 스크롤 라운드 수", ge=1, le=200),
):
    """GET 방식으로 스크래핑 요청
    
    예시: GET /scrape?username=zuck&max_posts=10
    """
    try:
        # @ 기호 제거 (사용자가 @를 포함해서 입력한 경우)
        username = username.lstrip("@")
        
        if not username:
            raise HTTPException(status_code=400, detail="사용자명이 필요합니다")
        
        posts = await scrape_threads_profile(
            username=username,
            max_posts=max_posts,
            max_scroll_rounds=max_scroll_rounds,
        )
        
        return ScrapeResponse(
            username=username,
            total_posts=len(posts),
            posts=[PostResponse(**post) for post in posts],
            scraped_at=datetime.now().isoformat(),
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
        "max_scroll_rounds": 50
    }
    ```
    """
    try:
        # @ 기호 제거 (사용자가 @를 포함해서 입력한 경우)
        username = request.username.lstrip("@")
        
        if not username:
            raise HTTPException(status_code=400, detail="사용자명이 필요합니다")
        
        posts = await scrape_threads_profile(
            username=username,
            max_posts=request.max_posts,
            max_scroll_rounds=request.max_scroll_rounds,
        )
        
        return ScrapeResponse(
            username=username,
            total_posts=len(posts),
            posts=[PostResponse(**post) for post in posts],
            scraped_at=datetime.now().isoformat(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스크래핑 중 오류 발생: {str(e)}")


async def scrape_single_account(
    username: str,
    max_posts: Optional[int],
    max_scroll_rounds: int,
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
                posts=[],
                error="사용자명이 비어있습니다",
                scraped_at=scraped_at,
            )
        
        posts = await scrape_threads_profile(
            username=username,
            max_posts=max_posts,
            max_scroll_rounds=max_scroll_rounds,
        )
        
        return BatchScrapeItem(
            username=username,
            success=True,
            total_posts=len(posts),
            posts=[PostResponse(**post) for post in posts],
            error=None,
            scraped_at=scraped_at,
        )
    except Exception as e:
        return BatchScrapeItem(
            username=username,
            success=False,
            total_posts=0,
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
        "max_scroll_rounds": 50
    }
    ```
    """
    try:
        if not request.usernames:
            raise HTTPException(status_code=400, detail="사용자명 리스트가 비어있습니다")
        
        # 중복 제거
        unique_usernames = list(dict.fromkeys(request.usernames))
        
        # 모든 계정을 병렬로 스크래핑
        tasks = [
            scrape_single_account(
                username=username,
                max_posts=request.max_posts,
                max_scroll_rounds=request.max_scroll_rounds,
            )
            for username in unique_usernames
        ]
        
        results = await asyncio.gather(*tasks)
        
        # 성공/실패 통계 계산
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


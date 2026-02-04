"""Threads 스크래퍼 API 서버"""
import asyncio
import json
import os
import secrets
import sqlite3
import traceback
from email.utils import format_datetime
import hashlib
import subprocess
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Literal

from fastapi import FastAPI, HTTPException, Query, Depends, Request
from fastapi import BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field
from dateutil import parser as date_parser

from scraper import (
    scrape_threads_profile,
    search_threads_users,
    scrape_thread_with_replies,
    scrape_threads_profile_with_replies,
)
from rss_sync import (
    ensure_schema as ensure_rss_schema,
    run_once as rss_run_once,
    LOG_PATH as RSS_LOG_PATH,
    DB_PATH as RSS_DB_PATH,
    refresh_rss_cache_for_account,
    load_accounts,
)

# 한국 표준 시간대 (KST = UTC+9)
KST = timezone(timedelta(hours=9))
UTC = timezone.utc

app = FastAPI(
    title="Threads Collector API",
    description="Threads 프로필 게시물을 스크래핑하는 API 서버",
    version="0.1.0",
)

security = HTTPBasic()


def _require_admin(credentials: HTTPBasicCredentials) -> None:
    admin_user = os.environ.get("ADMIN_USER", "admin")
    admin_password = os.environ.get("ADMIN_PASSWORD")
    if not admin_password:
        raise HTTPException(status_code=500, detail="ADMIN_PASSWORD is not set")
    if credentials.username != admin_user or credentials.password != admin_password:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _get_db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(RSS_DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    ensure_rss_schema(conn)
    return conn


def _tail_log(path: str, lines: int = 200) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = 4096
            data = b""
            while size > 0 and data.count(b"\n") <= lines:
                read_size = block if size - block > 0 else size
                f.seek(-read_size, os.SEEK_CUR)
                data = f.read(read_size) + data
                f.seek(-read_size, os.SEEK_CUR)
                size -= read_size
            text = data.decode("utf-8", errors="replace")
            return "\n".join(text.splitlines()[-lines:])
    except FileNotFoundError:
        return ""


def _get_schedule(conn: sqlite3.Connection, display_kst: bool = True) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT is_active, interval_minutes, start_time, last_run_at, updated_at FROM rss_schedule WHERE id = 1"
    ).fetchone()
    if not row:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO rss_schedule (id, is_active, interval_minutes, start_time, last_run_at, updated_at) "
            "VALUES (1, 1, 30, '09:00', NULL, ?)",
            (now,),
        )
        conn.commit()
        row = (1, 30, "09:00", None, now)
    start_time_utc = row[2]
    start_time_value = start_time_utc
    if display_kst:
        try:
            hh, mm = start_time_utc.split(":")
            kst_dt = datetime(2000, 1, 1, int(hh), int(mm), tzinfo=timezone.utc).astimezone(KST)
            start_time_value = f"{kst_dt.hour:02d}:{kst_dt.minute:02d}"
        except Exception:
            start_time_value = "09:00"
    return {
        "is_active": bool(row[0]),
        "interval_minutes": row[1],
        "start_time": start_time_value,
        "start_time_utc": start_time_utc,
        "last_run_at": row[3],
        "updated_at": row[4],
    }


def _get_rate_limit(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT window_seconds, max_requests, updated_at FROM rss_rate_limits WHERE id = 1"
    ).fetchone()
    if not row:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO rss_rate_limits (id, window_seconds, max_requests, updated_at) "
            "VALUES (1, 300, 60, ?)",
            (now,),
        )
        conn.commit()
        row = (300, 60, now)
    return {
        "window_seconds": row[0],
        "max_requests": row[1],
        "updated_at": row[2],
    }


def _get_cache_policy(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT enabled, ttl_seconds, updated_at FROM rss_cache_policy WHERE id = 1"
    ).fetchone()
    if not row:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO rss_cache_policy (id, enabled, ttl_seconds, updated_at) "
            "VALUES (1, 1, 300, ?)",
            (now,),
        )
        conn.commit()
        row = (1, 300, now)
    return {
        "enabled": bool(row[0]),
        "ttl_seconds": row[1],
        "updated_at": row[2],
    }


def _log_token_request(
    conn: sqlite3.Connection,
    token_id: int,
    username: str,
    request: Request,
    status_code: int,
) -> None:
    try:
        now = datetime.now(timezone.utc).isoformat()
        ip = request.client.host if request.client else None
        ua = request.headers.get("user-agent")
        conn.execute(
            "INSERT INTO rss_token_logs (token_id, username, ip, user_agent, status_code, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (token_id, username, ip, ua, status_code, now),
        )
        conn.commit()
    except Exception:
        pass


def _log_invalid_token(
    conn: sqlite3.Connection,
    token_value: str,
    username: Optional[str],
    request: Request,
    status_code: int,
) -> None:
    try:
        now = datetime.now(timezone.utc).isoformat()
        token_hash = hashlib.sha256(token_value.encode("utf-8")).hexdigest()
        ip = request.client.host if request.client else None
        ua = request.headers.get("user-agent")
        conn.execute(
            "INSERT INTO rss_invalid_token_logs (token_hash, username, ip, user_agent, status_code, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (token_hash, username, ip, ua, status_code, now),
        )
        conn.commit()
    except Exception:
        pass


def _compute_schedule_times(
    now_utc: datetime, start_time_utc: str, interval_minutes: int
) -> tuple[Optional[datetime], datetime]:
    try:
        hh, mm = start_time_utc.split(":")
        start_today = now_utc.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
    except Exception:
        start_today = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    if now_utc < start_today:
        return None, start_today
    delta_minutes = int((now_utc - start_today).total_seconds() // 60)
    steps = delta_minutes // max(1, interval_minutes)
    prev = start_today + timedelta(minutes=steps * interval_minutes)
    return prev, prev + timedelta(minutes=interval_minutes)


def _compute_next_run(schedule: Dict[str, Any], start_time_utc: str) -> Optional[str]:
    if not schedule.get("is_active"):
        return None
    now = datetime.now(timezone.utc)
    _, next_dt = _compute_schedule_times(now, start_time_utc, schedule.get("interval_minutes") or 30)
    return next_dt.astimezone(KST).isoformat()


async def _scheduler_loop() -> None:
    lock = asyncio.Lock()
    while True:
        await asyncio.sleep(30)
        try:
            conn = _get_db_conn()
            sched = _get_schedule(conn, display_kst=False)
            conn.close()
            if not sched["is_active"]:
                continue
            last = sched["last_run_at"]
            last_dt = None
            if last:
                try:
                    last_dt = date_parser.parse(last)
                except Exception:
                    last_dt = None
            now = datetime.now(timezone.utc)
            prev_due, _next_due = _compute_schedule_times(
                now, sched.get("start_time_utc") or "00:00", sched.get("interval_minutes") or 30
            )
            if prev_due is None:
                due = False
            else:
                if last_dt:
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    due = last_dt < prev_due
                else:
                    due = True
            if not due:
                continue
            if lock.locked():
                continue
            async with lock:
                await rss_run_once(None)
                conn = _get_db_conn()
                now_iso = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    "UPDATE rss_schedule SET last_run_at = ?, updated_at = ? WHERE id = 1",
                    (now_iso, now_iso),
                )
                conn.commit()
                conn.close()
        except Exception:
            continue


def _xml_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;")
    )


def _parse_media_json(raw: Optional[str]) -> List[Dict[str, Any]]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _guess_mime(url: str) -> str:
    lower = url.lower()
    if ".jpg" in lower or ".jpeg" in lower:
        return "image/jpeg"
    if ".png" in lower:
        return "image/png"
    if ".webp" in lower:
        return "image/webp"
    if ".gif" in lower:
        return "image/gif"
    if ".mp4" in lower:
        return "video/mp4"
    if ".webm" in lower:
        return "video/webm"
    return "application/octet-stream"


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


def compute_cutoff_utc(
    since_days: Optional[int] = None,
    since_date: Optional[str] = None,
) -> Optional[datetime]:
    """since_days/since_date로 cutoff(UTC, timezone-aware) 계산.
    
    - since_date가 있으면 since_days보다 우선
    - filter_posts_by_date와 동일한 의미를 유지
    """
    if since_date:
        parsed = parse_datetime(since_date)
        if not parsed:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=KST)
        return parsed.astimezone(UTC)
    if since_days:
        return datetime.now(UTC) - timedelta(days=since_days)
    return None


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
        # 현재 시간 기준으로 N*24시간 전부터 (더 직관적인 방식)
        cutoff = datetime.now(UTC) - timedelta(days=since_days)
        print(f"[filter] 현재 KST: {now_kst.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[filter] cutoff UTC: {cutoff.strftime('%Y-%m-%d %H:%M:%S')} (since_days={since_days})")
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
    for i, post in enumerate(posts):
        created_at = parse_datetime(post.get("created_at"))
        url = post.get("url", "")
        post_id = url.split("/post/")[-1] if "/post/" in url else f"post_{i}"
        
        if created_at is None:
            # 날짜 정보 없으면 포함 (보수적 처리)
            filtered.append(post)
            print(f"[filter] {post_id}: created_at=None → 포함")
        else:
            # UTC로 변환하여 비교
            created_at_aware = make_aware(created_at)
            if created_at_aware >= cutoff:
                filtered.append(post)
                print(f"[filter] {post_id}: {created_at_aware} >= {cutoff} → ✅ 포함")
            else:
                print(f"[filter] {post_id}: {created_at_aware} < {cutoff} → ❌ 제외")
    
    print(f"[filter] 결과: {len(posts)}개 중 {len(filtered)}개 통과")
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
    media: List[Dict[str, str]] = []


class ScrapeResponse(BaseModel):
    """스크래핑 응답 모델"""
    username: str
    total_posts: int
    filtered_posts: int
    posts: List[PostResponse]
    scraped_at: str
    filter_applied: Optional[str] = None


class BatchScrapeRequest(BaseModel):
    """배치 스크래핑 요청 모델 (레거시 호환용)"""
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


class BatchScrapeRequestV2(BaseModel):
    """배치 스크래핑 요청 모델"""
    usernames: List[str] = Field(..., description="Threads 사용자명 리스트 (예: ['zuck', 'meta'])", min_length=1, max_length=50)
    since_days: int = Field(1, description="최근 N일 이내 게시물만 (기본: 1일)", ge=1, le=365)
    since_date: Optional[str] = Field(None, description="특정 날짜 이후 게시물만 (ISO 형식, since_days보다 우선)")
    max_per_account: int = Field(10, description="계정별 최대 스크랩 갯수 (기본: 10)", ge=1, le=100)
    max_total: int = Field(300, description="전체 최대 스크랩 갯수 - 도달 시 중지 (기본: 300)", ge=1, le=1000)
    include_replies: bool = Field(True, description="답글 포함 여부 (기본: True)")
    max_reply_depth: int = Field(1, description="답글 수집 시 최대 깊이 (기본: 1)", ge=1, le=10)
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "usernames": ["zuck", "meta", "instagram"],
                    "max_per_account": 10,
                    "max_total": 300
                }
            ]
        }
    }


class V2BatchScrapeRequest(BaseModel):
    """새로운 응답 형식용 배치 스크래핑 요청 모델"""
    usernames: List[str] = Field(..., description="Threads 사용자명 리스트", min_length=1, max_length=50)
    since_days: Optional[int] = Field(1, description="최근 N일 이내 게시물만 (기본: 1일)", ge=1, le=365)
    since_date: Optional[str] = Field(None, description="특정 날짜 이후 게시물만 (since_days보다 우선)")
    include_replies: bool = Field(True, description="답글 포함 여부 (기본: True)")
    max_reply_depth: int = Field(1, description="답글 수집 시 최대 깊이 (기본: 1)", ge=1, le=10)
    max_per_account: int = Field(10, description="계정별 최대 스크랩 갯수 (기본: 10)", ge=1, le=100)
    max_total: int = Field(300, description="전체 최대 스크랩 갯수 (기본: 300)", ge=1, le=1000)
    response_style: Literal["threaded", "flat"] = Field(
        "threaded",
        description="응답 스타일: threaded(스레드 구조) | flat(플랫 구조)"
    )
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "usernames": ["zuck", "meta", "instagram"],
                    "since_days": 7,
                    "max_per_account": 10,
                    "max_total": 300,
                    "response_style": "threaded",
                }
            ]
        }
    }


class AdminAccountCreate(BaseModel):
    username: str = Field(..., description="Threads username")
    display_name: Optional[str] = None
    profile_url: Optional[str] = None
    include_replies: Optional[bool] = True
    max_reply_depth: Optional[int] = Field(1, ge=1, le=10)


class AdminAccountUpdate(BaseModel):
    is_active: Optional[bool] = None
    display_name: Optional[str] = None
    profile_url: Optional[str] = None
    include_replies: Optional[bool] = None
    max_reply_depth: Optional[int] = Field(None, ge=1, le=10)


class AdminScrapeRequest(BaseModel):
    usernames: Optional[List[str]] = None
    all_accounts: bool = False


class AdminScheduleUpdate(BaseModel):
    is_active: Optional[bool] = None
    interval_minutes: Optional[int] = Field(None, ge=5, le=1440)
    start_time: Optional[str] = Field(None, description="HH:MM 24h (KST)")


class AdminCacheRefreshRequest(BaseModel):
    usernames: Optional[List[str]] = None
    all_accounts: bool = False


class AdminTokenCreate(BaseModel):
    scope: str = Field(..., description="username or 'global'")
    is_active: bool = True


class AdminRateLimitUpdate(BaseModel):
    window_seconds: Optional[int] = Field(None, ge=60, le=3600)
    max_requests: Optional[int] = Field(None, ge=1, le=10000)


class AdminCachePolicyUpdate(BaseModel):
    enabled: Optional[bool] = None
    ttl_seconds: Optional[int] = Field(None, ge=30, le=86400)


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
    media: List[Dict[str, str]] = []
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
    media: List[Dict[str, str]] = []
    replies: List[ReplyPost] = []
    total_replies_count: int = 0
    scraped_at: str


class ScrapeThreadRequest(BaseModel):
    """개별 스레드 스크래핑 요청 모델"""
    post_url: str = Field(..., description="게시물 URL (예: 'https://www.threads.com/@user/post/ABC123')")
    max_depth: int = Field(1, description="최대 답글 깊이 (기본: 루트 + 직접 reply)", ge=1, le=10)
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
    max_reply_depth: int = Field(1, description="최대 답글 깊이 (기본: 루트 + 직접 reply)", ge=1, le=10)
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


class V2ScrapeRequest(BaseModel):
    """새로운 응답 형식용 스크래핑 요청 모델"""
    username: str = Field(..., description="Threads 사용자명 (예: 'zuck', '@' 없이 입력)")
    since_days: Optional[int] = Field(1, description="최근 N일 이내 게시물만 (기본: 1일)", ge=1, le=365)
    since_date: Optional[str] = Field(None, description="특정 날짜 이후 게시물만 (ISO 형식, since_days보다 우선)")
    include_replies: bool = Field(True, description="답글 포함 여부 (기본: True)")
    max_reply_depth: int = Field(1, description="답글 수집 시 최대 깊이 (기본: 루트 + 직접 reply)", ge=1, le=10)
    max_total_posts: int = Field(100, description="전체 출력 최대 게시물 수 (모든 root + 모든 답글 합계, 기본: 100)", ge=1, le=1000)
    response_style: Literal["threaded", "flat"] = Field(
        "threaded",
        description="응답 스타일: threaded(스레드 구조) | flat(플랫 구조)"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "username": "zuck",
                    "since_days": 7,
                    "include_replies": True,
                    "max_reply_depth": 2,
                    "max_total_posts": 100,
                    "response_style": "threaded",
                }
            ]
        }
    }


def build_v2_data_from_result(
    result: Dict[str, Any],
    response_style: Literal["threaded", "flat"],
    include_replies: bool,
) -> Dict[str, Any]:
    def normalize_post(post: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": post.get("post_id"),
            "url": post.get("url"),
            "text": post.get("text", ""),
            "created_at": post.get("created_at"),
            "author": post.get("author"),
            "media": post.get("media", []),
        }
    
    def normalize_simple_post(post: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": None,
            "url": post.get("url"),
            "text": post.get("text", ""),
            "created_at": post.get("created_at"),
            "author": None,
            "media": post.get("media", []),
        }
    
    def flatten_replies(replies: List[Dict[str, Any]], parent_id: Optional[str]) -> List[Dict[str, Any]]:
        flat = []
        for r in replies:
            flat.append({
                "post": {
                    "id": r.get("post_id"),
                    "url": r.get("url"),
                    "text": r.get("text", ""),
                    "created_at": r.get("created_at"),
                    "author": r.get("author"),
                    "media": r.get("media", []),
                },
                "parent_id": parent_id,
            })
            child_replies = r.get("replies", [])
            if child_replies:
                flat.extend(flatten_replies(child_replies, r.get("post_id")))
        return flat
    
    if include_replies:
        threads = []
        posts_flat = []
        for post in result["posts"]:
            root = normalize_post(post)
            replies = post.get("replies", [])
            
            if response_style == "flat":
                posts_flat.append({
                    "type": "root",
                    "thread_id": root["id"],
                    "post": root,
                    "parent_id": None,
                })
                for r in flatten_replies(replies, root["id"]):
                    posts_flat.append({
                        "type": "reply",
                        "thread_id": root["id"],
                        "post": r["post"],
                        "parent_id": r["parent_id"],
                    })
            else:
                threads.append({
                    "thread_id": root["id"],
                    "post": root,
                    "replies": replies,
                    "counts": {
                        "replies": post.get("total_replies_count", 0),
                        "total": 1 + post.get("total_replies_count", 0),
                    },
                })
        
        if response_style == "flat":
            return {"style": "flat", "items": posts_flat}
        return {"style": "threaded", "items": threads}
    
    # 답글 미포함
    threads = []
    posts_flat = []
    for post in result["posts"]:
        root = normalize_simple_post(post)
        if response_style == "flat":
            posts_flat.append({
                "type": "root",
                "thread_id": None,
                "post": root,
                "parent_id": None,
            })
        else:
            threads.append({
                "thread_id": None,
                "post": root,
                "replies": [],
                "counts": {"replies": 0, "total": 1},
            })
    
    if response_style == "flat":
        return {"style": "flat", "items": posts_flat}
    return {"style": "threaded", "items": threads}


@app.on_event("startup")
async def on_startup():
    if os.environ.get("ENABLE_INTERNAL_SCHEDULER") == "1":
        asyncio.create_task(_scheduler_loop())


@app.get("/v2/rss")
def rss_feed(
    request: Request,
    username: str = Query(..., description="Threads username"),
    token: str = Query(..., description="Access token"),
    limit: int = Query(50, ge=1, le=500),
):
    username = username.lstrip("@").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username is required")
    conn = _get_db_conn()
    try:
        tok = conn.execute(
            "SELECT id, token, scope, is_active FROM tokens WHERE token = ?",
            (token,),
        ).fetchone()
        if not tok or not tok[2]:
            _log_invalid_token(conn, token, username, request, 401)
            raise HTTPException(status_code=401, detail="invalid token")
        token_id = tok[0]
        scope = tok[2]
        if scope not in ("global", "*", username):
            _log_token_request(conn, token_id, username, request, 403)
            raise HTTPException(status_code=403, detail="token scope mismatch")

        # rate limit
        rl = _get_rate_limit(conn)
        window = int(datetime.now(timezone.utc).timestamp()) // rl["window_seconds"]
        row = conn.execute(
            "SELECT count FROM rss_token_counters WHERE token_id = ? AND window_start = ?",
            (token_id, window),
        ).fetchone()
        if row and row[0] >= rl["max_requests"]:
            _log_token_request(conn, token_id, username, request, 429)
            raise HTTPException(status_code=429, detail="rate limit exceeded")
        if row:
            conn.execute(
                "UPDATE rss_token_counters SET count = count + 1 WHERE token_id = ? AND window_start = ?",
                (token_id, window),
            )
        else:
            conn.execute(
                "INSERT INTO rss_token_counters (token_id, window_start, count) VALUES (?, ?, 1)",
                (token_id, window),
            )
        conn.commit()
        
        src = conn.execute(
            "SELECT id, username FROM feed_sources WHERE username = ?",
            (username,),
        ).fetchone()
        if not src:
            raise HTTPException(status_code=404, detail="account not found")
        
        roots = conn.execute(
            "SELECT post_id, url, text, media_json, created_at FROM posts "
            "WHERE source_id = ? AND is_reply = 0 ORDER BY created_at DESC LIMIT ?",
            (src[0], limit),
        ).fetchall()
        
        channel_title = f"{username} Threads Feed"
        channel_link = f"https://www.threads.com/@{username}"
        channel_desc = f"Threads posts scraped for @{username}"
        
        items = []
        for r in roots:
            post_id, url, text, media_json, created_at = r
            root_media = _parse_media_json(media_json)
            replies = conn.execute(
                "SELECT text, media_json FROM posts WHERE source_id = ? AND is_reply = 1 AND parent_post_id = ? "
                "ORDER BY created_at ASC",
                (src[0], post_id),
            ).fetchall()
            parts = []
            if (text or "").strip():
                parts.append((text or "").strip())
            media_urls = []
            if root_media:
                media_urls.extend([m.get("url") for m in root_media if m.get("url")])
            for rr in replies:
                rep_text = (rr[0] or "").strip()
                if rep_text:
                    parts.append(rep_text)
                rep_media = _parse_media_json(rr[1])
                if rep_media:
                    media_urls.extend([m.get("url") for m in rep_media if m.get("url")])
            # dedupe media
            seen_media = set()
            media_urls = [u for u in media_urls if u and not (u in seen_media or seen_media.add(u))]
            text = "\n\n".join([p for p in parts if p])
            created_dt = None
            try:
                created_dt = date_parser.parse(created_at) if created_at else None
            except Exception:
                created_dt = None
            if created_dt and created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=timezone.utc)
            pub_date = format_datetime(created_dt) if created_dt else format_datetime(datetime.now(timezone.utc))
            title = (text or "").strip().split("\n")[0][:80] or "(no title)"
            desc = (text or "").strip()
            enclosures = "".join(
                f"<enclosure url=\"{_xml_escape(mu)}\" length=\"0\" type=\"{_xml_escape(_guess_mime(mu))}\" />"
                for mu in media_urls
            )
            items.append(
                f"<item>"
                f"<title>{_xml_escape(title)}</title>"
                f"<link>{_xml_escape(url)}</link>"
                f"<guid>{_xml_escape(post_id or url)}</guid>"
                f"<pubDate>{pub_date}</pubDate>"
                f"<description>{_xml_escape(desc)}</description>"
                f"{enclosures}"
                f"</item>"
            )
        
        cache_policy = _get_cache_policy(conn)
        cache_row = None
        if cache_policy["enabled"]:
            cache_row = conn.execute(
                "SELECT etag, last_modified, xml, updated_at FROM rss_feed_cache WHERE username = ? AND limit_count = ?",
                (username, limit),
            ).fetchone()
        if cache_row:
            cached_etag, cached_last, cached_xml, cached_updated = cache_row
            cache_ok = True
            if cache_policy["ttl_seconds"] > 0 and cached_updated:
                try:
                    updated_dt = date_parser.parse(cached_updated)
                    if updated_dt.tzinfo is None:
                        updated_dt = updated_dt.replace(tzinfo=timezone.utc)
                    cache_ok = datetime.now(timezone.utc) - updated_dt <= timedelta(seconds=cache_policy["ttl_seconds"])
                except Exception:
                    cache_ok = False
            if cache_ok:
                inm = request.headers.get("if-none-match")
                ims = request.headers.get("if-modified-since")
                if inm and inm == cached_etag:
                    _log_token_request(conn, token_id, username, request, 304)
                    return PlainTextResponse("", status_code=304)
                if ims and cached_last and ims == cached_last:
                    _log_token_request(conn, token_id, username, request, 304)
                    return PlainTextResponse("", status_code=304)
                headers = {"ETag": cached_etag}
                if cached_last:
                    headers["Last-Modified"] = cached_last
                _log_token_request(conn, token_id, username, request, 200)
                return PlainTextResponse(cached_xml, media_type="application/rss+xml", headers=headers)

        xml = (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<rss version=\"2.0\">"
            "<channel>"
            f"<title>{_xml_escape(channel_title)}</title>"
            f"<link>{_xml_escape(channel_link)}</link>"
            f"<description>{_xml_escape(channel_desc)}</description>"
            + "".join(items)
            + "</channel></rss>"
        )
        etag = hashlib.sha256(xml.encode("utf-8")).hexdigest()
        last_modified = None
        if roots:
            try:
                last_modified_dt = date_parser.parse(roots[0][4]) if roots[0][4] else None
                if last_modified_dt and last_modified_dt.tzinfo is None:
                    last_modified_dt = last_modified_dt.replace(tzinfo=timezone.utc)
                if last_modified_dt:
                    last_modified = format_datetime(last_modified_dt)
            except Exception:
                last_modified = None
        inm = request.headers.get("if-none-match")
        ims = request.headers.get("if-modified-since")
        if inm and inm == etag:
            _log_token_request(conn, token_id, username, request, 304)
            return PlainTextResponse("", status_code=304)
        if ims and last_modified and ims == last_modified:
            _log_token_request(conn, token_id, username, request, 304)
            return PlainTextResponse("", status_code=304)

        headers = {"ETag": etag}
        if last_modified:
            headers["Last-Modified"] = last_modified
        _log_token_request(conn, token_id, username, request, 200)
        if cache_policy["enabled"]:
            try:
                now = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    "INSERT OR REPLACE INTO rss_feed_cache (username, limit_count, etag, last_modified, xml, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (username, limit, etag, last_modified, xml, now),
                )
                conn.commit()
            except Exception:
                pass
        return PlainTextResponse(xml, media_type="application/rss+xml", headers=headers)
    finally:
        conn.close()


@app.get("/")
async def root():
    """API 루트 엔드포인트"""
    return {
        "message": "Threads Collector API",
        "version": "0.4.0",
        "endpoints": {
            "GET /scrape": "⭐ 단일 계정 스크래핑 - 메타데이터 포함",
            "POST /batch-scrape": "⭐ 배치 스크래핑 - 여러 계정 동시 처리 (최대 5개 병렬)",
            "POST /v2/scrape": "🧼 새 응답 형식 - 단일 계정 스크래핑",
            "POST /v2/batch-scrape": "🧼 새 응답 형식 - 배치 스크래핑",
            "GET /v2/rss": "🧾 계정별 RSS 피드 (token 필요)",
            "GET /admin": "🔐 관리자 UI",
            "GET /search-users": "사용자 검색 (자동완성)",
            "GET /scrape-thread": "개별 게시물과 답글 수집",
            "GET /health": "서버 상태 확인",
        },
        "quick_start": {
            "single": "GET /scrape?username=zuck&since_days=7",
            "batch": "POST /batch-scrape {\"usernames\": [\"zuck\", \"meta\"], \"since_days\": 7}",
        },
        "response_structure": {
            "single": "{ meta, stats, date_range, filter, posts }",
            "batch": "{ total_meta, results: [{ meta, stats, ... }, ...] }",
            "v2": "{ meta, filter, stats, data }",
        },
    }


@app.get("/admin")
def admin_ui(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    return FileResponse("admin/index.html")


@app.get("/admin/tokens")
def admin_tokens_ui(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    return FileResponse("admin/tokens.html")


@app.get("/admin/settings")
def admin_settings_ui(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    return FileResponse("admin/settings.html")


@app.get("/admin/posts")
def admin_posts_ui(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    return FileResponse("admin/posts.html")


@app.get("/admin/db")
def admin_db_ui(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    return FileResponse("admin/db.html")


@app.get("/admin/api/accounts")
def admin_list_accounts(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        rows = conn.execute(
            "SELECT id, username, display_name, profile_url, is_active, include_replies, max_reply_depth, created_at "
            "FROM feed_sources ORDER BY id ASC"
        ).fetchall()
        data = [
            {
                "id": r[0],
                "username": r[1],
                "display_name": r[2],
                "profile_url": r[3],
                "is_active": bool(r[4]),
                "include_replies": bool(r[5]),
                "max_reply_depth": r[6],
                "created_at": r[7],
            }
            for r in rows
        ]
        return {"accounts": data}
    finally:
        conn.close()


@app.post("/admin/api/accounts")
def admin_add_account(payload: AdminAccountCreate, credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    username = payload.username.lstrip("@").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username is required")
    profile_url = payload.profile_url or f"https://www.threads.com/@{username}"
    conn = _get_db_conn()
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO feed_sources "
            "(username, display_name, profile_url, include_replies, max_reply_depth, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                username,
                payload.display_name,
                profile_url,
                1 if payload.include_replies else 0,
                payload.max_reply_depth or 1,
                now,
            ),
        )
        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()


@app.patch("/admin/api/accounts/{account_id}")
def admin_update_account(
    account_id: int,
    payload: AdminAccountUpdate,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    fields = []
    values = []
    if payload.is_active is not None:
        fields.append("is_active = ?")
        values.append(1 if payload.is_active else 0)
    if payload.include_replies is not None:
        fields.append("include_replies = ?")
        values.append(1 if payload.include_replies else 0)
    if payload.max_reply_depth is not None:
        fields.append("max_reply_depth = ?")
        values.append(payload.max_reply_depth)
    if payload.display_name is not None:
        fields.append("display_name = ?")
        values.append(payload.display_name)
    if payload.profile_url is not None:
        fields.append("profile_url = ?")
        values.append(payload.profile_url)
    if not fields:
        return {"status": "no_change"}
    values.append(account_id)
    conn = _get_db_conn()
    try:
        conn.execute(
            f"UPDATE feed_sources SET {', '.join(fields)} WHERE id = ?",
            tuple(values),
        )
        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()


@app.delete("/admin/api/accounts/{account_id}")
def admin_delete_account(account_id: int, credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        conn.execute("DELETE FROM feed_sources WHERE id = ?", (account_id,))
        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()


@app.post("/admin/api/scrape")
def admin_scrape(
    payload: AdminScrapeRequest,
    background_tasks: BackgroundTasks,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    if payload.all_accounts:
        background_tasks.add_task(rss_run_once, None)
    else:
        if not payload.usernames:
            raise HTTPException(status_code=400, detail="usernames is required when all_accounts=false")
        background_tasks.add_task(rss_run_once, payload.usernames)
    return {"status": "started"}


@app.get("/admin/api/logs")
def admin_logs(lines: int = 200, credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    text = _tail_log(RSS_LOG_PATH, lines=lines)
    return PlainTextResponse(text)


@app.post("/admin/api/rss-cache/refresh")
def admin_refresh_rss_cache(
    payload: AdminCacheRefreshRequest,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        accounts = load_accounts(conn, only_active=True)
        if payload.all_accounts:
            targets = accounts
        else:
            if not payload.usernames:
                raise HTTPException(status_code=400, detail="usernames is required when all_accounts=false")
            usernames_set = {u.lstrip("@") for u in payload.usernames}
            targets = [a for a in accounts if a["username"] in usernames_set]
        for acc in targets:
            refresh_rss_cache_for_account(conn, acc["id"], acc["username"])
        return {"status": "ok", "updated": len(targets)}
    finally:
        conn.close()


ALLOWED_DB_TABLES = {
    "feed_sources",
    "posts",
    "tokens",
    "rss_token_logs",
    "rss_invalid_token_logs",
    "rss_rate_limits",
    "rss_token_counters",
    "rss_feed_cache",
    "rss_cache_policy",
    "rss_schedule",
}


@app.get("/admin/api/db/tables")
def admin_db_tables(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    return {"tables": sorted(ALLOWED_DB_TABLES)}


@app.get("/admin/api/db/rows")
def admin_db_rows(
    table: str,
    limit: int = 50,
    offset: int = 0,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    if table not in ALLOWED_DB_TABLES:
        raise HTTPException(status_code=400, detail="invalid table")
    limit = max(1, min(500, limit))
    offset = max(0, offset)
    conn = _get_db_conn()
    try:
        cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
        columns = [c[1] for c in cols]
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        rows = conn.execute(
            f"SELECT * FROM {table} LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        return {"columns": columns, "rows": rows, "total": total}
    finally:
        conn.close()


class AdminDbDelete(BaseModel):
    table: str


@app.post("/admin/api/db/delete")
def admin_db_delete(
    payload: AdminDbDelete,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    table = payload.table
    if table not in ALLOWED_DB_TABLES:
        raise HTTPException(status_code=400, detail="invalid table")
    conn = _get_db_conn()
    try:
        conn.execute(f"DELETE FROM {table}")
        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()


@app.get("/admin/api/posts")
def admin_list_posts(
    username: Optional[str] = None,
    type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    limit = max(1, min(500, limit))
    offset = max(0, offset)
    username_value = username.lstrip("@").strip() if username else None
    type_value = (type or "all").lower()
    where = []
    params: List[Any] = []
    if username_value:
        where.append("s.username = ?")
        params.append(username_value)
    if type_value == "root":
        where.append("p.is_reply = 0")
    elif type_value == "reply":
        where.append("p.is_reply = 1")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    conn = _get_db_conn()
    try:
        total = conn.execute(
            f"SELECT COUNT(*) FROM posts p JOIN feed_sources s ON s.id = p.source_id {where_sql}",
            tuple(params),
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT p.post_id, p.url, p.text, p.media_json, p.created_at, p.is_reply, s.username "
            "FROM posts p JOIN feed_sources s ON s.id = p.source_id "
            f"{where_sql} ORDER BY p.created_at DESC LIMIT ? OFFSET ?",
            tuple(params + [limit, offset]),
        ).fetchall()
        return {
            "total": total,
            "posts": [
                {
                    "post_id": r[0],
                    "url": r[1],
                    "text": r[2],
                    "media": _parse_media_json(r[3]),
                    "created_at": r[4],
                    "is_reply": bool(r[5]),
                    "username": r[6],
                }
                for r in rows
            ],
        }
    finally:
        conn.close()


@app.get("/admin/api/schedule")
def admin_get_schedule(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        sched = _get_schedule(conn, display_kst=True)
        return {**sched, "next_run_at": _compute_next_run(sched, sched.get("start_time_utc") or "00:00")}
    finally:
        conn.close()


@app.patch("/admin/api/schedule")
def admin_update_schedule(
    payload: AdminScheduleUpdate,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        fields = []
        values = []
        interval_minutes = None
        if payload.is_active is not None:
            fields.append("is_active = ?")
            values.append(1 if payload.is_active else 0)
        if payload.interval_minutes is not None:
            interval_minutes = payload.interval_minutes
            fields.append("interval_minutes = ?")
            values.append(payload.interval_minutes)
        if payload.start_time is not None:
            # Convert KST HH:MM -> UTC HH:MM for storage
            try:
                hh, mm = payload.start_time.split(":")
                kst_dt = datetime(2000, 1, 1, int(hh), int(mm), tzinfo=KST)
                utc_dt = kst_dt.astimezone(timezone.utc)
                utc_hhmm = f"{utc_dt.hour:02d}:{utc_dt.minute:02d}"
            except Exception:
                utc_hhmm = "00:00"
            fields.append("start_time = ?")
            values.append(utc_hhmm)
        if not fields:
            return {"status": "no_change"}
        now = datetime.now(timezone.utc).isoformat()
        fields.append("updated_at = ?")
        values.append(now)
        values.append(1)
        conn.execute(
            f"UPDATE rss_schedule SET {', '.join(fields)} WHERE id = ?",
            tuple(values),
        )
        conn.commit()
        if interval_minutes is not None:
            try:
                conn.execute(
                    "UPDATE rss_cache_policy SET ttl_seconds = ?, updated_at = ? WHERE id = 1",
                    (interval_minutes * 60, now),
                )
                conn.commit()
            except Exception:
                pass
        # Optional: update systemd timer if enabled
        if _systemd_allowed():
            try:
                sched = _get_schedule(conn)
                _write_systemd_timer(sched)
                subprocess.run(["systemctl", "daemon-reload"], check=False)
                subprocess.run(["systemctl", "restart", "thread-collector-rss.timer"], check=False)
            except Exception:
                pass
        return {"status": "ok"}
    finally:
        conn.close()


@app.get("/admin/api/tokens")
def admin_list_tokens(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        rows = conn.execute(
            "SELECT id, token, scope, is_active, created_at FROM tokens ORDER BY id DESC"
        ).fetchall()
        return {
            "tokens": [
                {
                    "id": r[0],
                    "token": r[1],
                    "scope": r[2],
                    "is_active": bool(r[3]),
                    "created_at": r[4],
                }
                for r in rows
            ]
        }
    finally:
        conn.close()


@app.post("/admin/api/tokens")
def admin_create_token(
    payload: AdminTokenCreate,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    scope = payload.scope.strip()
    if not scope:
        raise HTTPException(status_code=400, detail="scope is required")
    token = secrets.token_urlsafe(24)
    conn = _get_db_conn()
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO tokens (token, scope, created_at, is_active) VALUES (?, ?, ?, ?)",
            (token, scope, now, 1 if payload.is_active else 0),
        )
        conn.commit()
        return {"token": token}
    finally:
        conn.close()


@app.patch("/admin/api/tokens/{token_id}")
def admin_update_token(
    token_id: int,
    is_active: bool,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        conn.execute(
            "UPDATE tokens SET is_active = ? WHERE id = ?",
            (1 if is_active else 0, token_id),
        )
        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()


@app.delete("/admin/api/tokens/{token_id}")
def admin_delete_token(token_id: int, credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        conn.execute("DELETE FROM tokens WHERE id = ?", (token_id,))
        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()


@app.get("/admin/api/rate-limit")
def admin_get_rate_limit(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        return _get_rate_limit(conn)
    finally:
        conn.close()


@app.patch("/admin/api/rate-limit")
def admin_update_rate_limit(
    payload: AdminRateLimitUpdate,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        fields = []
        values = []
        if payload.window_seconds is not None:
            fields.append("window_seconds = ?")
            values.append(payload.window_seconds)
        if payload.max_requests is not None:
            fields.append("max_requests = ?")
            values.append(payload.max_requests)
        if not fields:
            return {"status": "no_change"}
        now = datetime.now(timezone.utc).isoformat()
        fields.append("updated_at = ?")
        values.append(now)
        values.append(1)
        conn.execute(
            f"UPDATE rss_rate_limits SET {', '.join(fields)} WHERE id = ?",
            tuple(values),
        )
        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()


@app.get("/admin/api/token-logs")
def admin_token_logs(
    limit: int = 200,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        rows = conn.execute(
            "SELECT l.id, l.token_id, t.scope, l.username, l.ip, l.user_agent, l.status_code, l.created_at "
            "FROM rss_token_logs l "
            "JOIN tokens t ON t.id = l.token_id "
            "ORDER BY l.id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return {
            "logs": [
                {
                    "id": r[0],
                    "token_id": r[1],
                    "scope": r[2],
                    "username": r[3],
                    "ip": r[4],
                    "user_agent": r[5],
                    "status_code": r[6],
                    "created_at": r[7],
                }
                for r in rows
            ]
        }
    finally:
        conn.close()


@app.get("/admin/api/token-stats")
def admin_token_stats(
    hours: int = 24,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    if hours < 1 or hours > 168:
        raise HTTPException(status_code=400, detail="hours must be 1~168")
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    conn = _get_db_conn()
    try:
        rows = conn.execute(
            "SELECT t.id, t.scope, COUNT(*) as cnt "
            "FROM rss_token_logs l "
            "JOIN tokens t ON t.id = l.token_id "
            "WHERE l.created_at >= ? "
            "GROUP BY t.id, t.scope "
            "ORDER BY cnt DESC",
            (since.isoformat(),),
        ).fetchall()
        return {
            "hours": hours,
            "stats": [
                {"token_id": r[0], "scope": r[1], "count": r[2]}
                for r in rows
            ],
        }
    finally:
        conn.close()


@app.get("/admin/api/invalid-token-logs")
def admin_invalid_token_logs(
    limit: int = 200,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        rows = conn.execute(
            "SELECT id, token_hash, username, ip, user_agent, status_code, created_at "
            "FROM rss_invalid_token_logs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return {
            "logs": [
                {
                    "id": r[0],
                    "token_hash": r[1],
                    "username": r[2],
                    "ip": r[3],
                    "user_agent": r[4],
                    "status_code": r[5],
                    "created_at": r[6],
                }
                for r in rows
            ]
        }
    finally:
        conn.close()


@app.post("/admin/api/rss-cache/clear")
def admin_clear_rss_cache(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        conn.execute("DELETE FROM rss_feed_cache")
        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()


@app.get("/admin/api/rss-cache-policy")
def admin_get_rss_cache_policy(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        return _get_cache_policy(conn)
    finally:
        conn.close()


@app.patch("/admin/api/rss-cache-policy")
def admin_update_rss_cache_policy(
    payload: AdminCachePolicyUpdate,
    credentials: HTTPBasicCredentials = Depends(security),
):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        fields = []
        values = []
        if payload.ttl_seconds is not None:
            fields.append("ttl_seconds = ?")
            values.append(payload.ttl_seconds)
        if payload.enabled is not None:
            fields.append("enabled = ?")
            values.append(1 if payload.enabled else 0)
        if not fields:
            return {"status": "no_change"}
        now = datetime.now(timezone.utc).isoformat()
        fields.append("updated_at = ?")
        values.append(now)
        values.append(1)
        conn.execute(
            f"UPDATE rss_cache_policy SET {', '.join(fields)} WHERE id = ?",
            tuple(values),
        )
        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()


@app.get("/admin/api/token-logs.csv")
def admin_token_logs_csv(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    conn = _get_db_conn()
    try:
        rows = conn.execute(
            "SELECT l.id, l.token_id, t.scope, l.username, l.ip, l.user_agent, l.status_code, l.created_at "
            "FROM rss_token_logs l "
            "JOIN tokens t ON t.id = l.token_id "
            "ORDER BY l.id DESC"
        ).fetchall()
        lines = [
            "id,token_id,scope,username,ip,user_agent,status_code,created_at"
        ]
        for r in rows:
            row = [
                str(r[0]),
                str(r[1]),
                r[2] or "",
                r[3] or "",
                r[4] or "",
                (r[5] or "").replace("\n", " ").replace("\r", " "),
                str(r[6]),
                r[7] or "",
            ]
            lines.append(",".join([f"\"{c}\"" if "," in c else c for c in row]))
        csv_text = "\n".join(lines)
        return PlainTextResponse(csv_text, media_type="text/csv")
    finally:
        conn.close()


def _systemd_allowed() -> bool:
    return os.environ.get("ENABLE_SYSTEMD_CONTROL") == "1"

def _write_systemd_timer(schedule: Dict[str, Any]) -> None:
    interval_minutes = schedule.get("interval_minutes", 30)
    start_time_utc = schedule.get("start_time_utc") or schedule.get("start_time") or "00:00"
    timer_path = os.environ.get(
        "SYSTEMD_TIMER_PATH",
        "/etc/systemd/system/thread-collector-rss.timer",
    )
    on_calendar = None
    try:
        hh, mm = start_time_utc.split(":")
        hh_i = int(hh)
        mm_i = int(mm)
    except Exception:
        hh_i, mm_i = 0, 0

    if interval_minutes > 0 and 60 % interval_minutes == 0:
        # Run from start_time hour to end of day, aligned by minute offset
        on_calendar = f"*-*-* {hh_i:02d}..23:{mm_i:02d}/{interval_minutes}"
        content = (
            "[Unit]\n"
            "Description=Thread Collector RSS Sync Timer\n\n"
            "[Timer]\n"
            f"OnCalendar={on_calendar}\n"
            "AccuracySec=1min\n"
            "Persistent=true\n\n"
            "[Install]\n"
            "WantedBy=timers.target\n"
        )
    else:
        # Fallback: start at specific time, then repeat by interval
        on_calendar = f"*-*-* {hh_i:02d}:{mm_i:02d}:00"
        content = (
            "[Unit]\n"
            "Description=Thread Collector RSS Sync Timer\n\n"
            "[Timer]\n"
            f"OnCalendar={on_calendar}\n"
            f"OnUnitActiveSec={interval_minutes}min\n"
            "AccuracySec=1min\n"
            "Persistent=true\n\n"
            "[Install]\n"
            "WantedBy=timers.target\n"
        )
    with open(timer_path, "w", encoding="utf-8") as f:
        f.write(content)


@app.get("/admin/api/systemd/status")
def admin_systemd_status(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    if not _systemd_allowed():
        raise HTTPException(status_code=403, detail="systemd control disabled")
    res = subprocess.run(
        ["systemctl", "status", "thread-collector-rss.timer", "--no-pager"],
        capture_output=True,
        text=True,
    )
    return PlainTextResponse(res.stdout + res.stderr)


@app.post("/admin/api/systemd/start")
def admin_systemd_start(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    if not _systemd_allowed():
        raise HTTPException(status_code=403, detail="systemd control disabled")
    subprocess.run(["systemctl", "start", "thread-collector-rss.timer"], check=False)
    return {"status": "ok"}


@app.post("/admin/api/systemd/stop")
def admin_systemd_stop(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    if not _systemd_allowed():
        raise HTTPException(status_code=403, detail="systemd control disabled")
    subprocess.run(["systemctl", "stop", "thread-collector-rss.timer"], check=False)
    return {"status": "ok"}


@app.post("/admin/api/systemd/restart")
def admin_systemd_restart(credentials: HTTPBasicCredentials = Depends(security)):
    _require_admin(credentials)
    if not _systemd_allowed():
        raise HTTPException(status_code=403, detail="systemd control disabled")
    subprocess.run(["systemctl", "restart", "thread-collector-rss.timer"], check=False)
    return {"status": "ok"}

@app.get("/health")
async def health():
    """서버 상태 확인"""
    return {"status": "healthy"}


# ============ 답글 포함 스크래핑 엔드포인트 ============

@app.get("/scrape-thread", response_model=ThreadWithRepliesResponse)
async def scrape_thread_get(
    url: str = Query(..., description="게시물 URL (예: 'https://www.threads.com/@user/post/ABC123')"),
    max_depth: int = Query(1, description="최대 답글 깊이 (기본: 루트 + 직접 reply)", ge=1, le=10),
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
                    media=r.get("media", []),
                    replies=convert_replies(r.get("replies", [])),
                ))
            return result
        
        return ThreadWithRepliesResponse(
            post_id=thread_data.get("post_id"),
            text=thread_data.get("text", ""),
            created_at=thread_data.get("created_at"),
            url=thread_data.get("url", ""),
            author=thread_data.get("author"),
            media=thread_data.get("media", []),
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
                    media=r.get("media", []),
                    replies=convert_replies(r.get("replies", [])),
                ))
            return result
        
        return ThreadWithRepliesResponse(
            post_id=thread_data.get("post_id"),
            text=thread_data.get("text", ""),
            created_at=thread_data.get("created_at"),
            url=thread_data.get("url", ""),
            author=thread_data.get("author"),
            media=thread_data.get("media", []),
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
            cutoff_utc=compute_cutoff_utc(since_days=request.since_days, since_date=request.since_date),
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
                    media=r.get("media", []),
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
                media=post.get("media", []),
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
    max_reply_depth: int = Query(1, description="답글 수집 시 최대 깊이 (기본: 루트 + 직접 reply)", ge=1, le=10),
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
    
    응답 형식:
    - 배열의 첫 번째 요소: 메타데이터 (_type: "metadata")
    - 배열의 두 번째 요소: 기존 응답 데이터
    """
    try:
        import time
        scrape_start_time = time.time()
        
        username = username.lstrip("@")
        
        if not username:
            raise HTTPException(status_code=400, detail="사용자명이 필요합니다")
        
        # 1단계: 프로필에서 게시물 목록 먼저 수집 (날짜 정보 포함)
        cutoff_utc = compute_cutoff_utc(since_days=since_days, since_date=since_date)
        scrape_result = await scrape_threads_profile(
            username=username,
            max_posts=None,  # 제한 없음
            max_scroll_rounds=200,
            cutoff_utc=cutoff_utc,
        )
        profile_posts = scrape_result["posts"]
        profile_scroll_rounds = scrape_result["scroll_rounds"]
        profile_duration = scrape_result["duration_seconds"]
        
        total_before_filter = len(profile_posts)
        print(f"[API] 프로필에서 수집된 게시물: {total_before_filter}개")
        
        # 2단계: 날짜 필터 적용 (since_date가 있으면 우선)
        if since_date:
            filtered_posts = filter_posts_by_date(profile_posts, since_date=since_date)
            filter_desc = f"{since_date} 이후"
        else:
            filtered_posts = filter_posts_by_date(profile_posts, since_days=since_days)
            filter_desc = f"최근 {since_days}일 이내"
        
        print(f"[API] 날짜 필터링 후: {len(filtered_posts)}개 ({filter_desc})")
        
        # 3단계: 답글 포함 시 필터링된 게시물에 대해서만 답글 수집
        if include_replies:
            import asyncio as _asyncio
            
            def convert_replies(replies: List[Dict]) -> List[ReplyPost]:
                result = []
                for r in replies:
                    result.append(ReplyPost(
                        post_id=r.get("post_id"),
                        text=r.get("text", ""),
                        created_at=r.get("created_at"),
                        url=r.get("url", ""),
                        author=r.get("author"),
                        media=r.get("media", []),
                        replies=convert_replies(r.get("replies", [])),
                    ))
                return result
            
            thread_responses = []
            total_collected = 0
            seen_root_post_ids: set[str] = set()
            
            for i, post in enumerate(filtered_posts):
                # 최대 게시물 수 도달 시 중단
                if total_collected >= max_total_posts:
                    print(f"[API] 최대 게시물 수({max_total_posts}) 도달, 수집 중단")
                    break
                
                post_url = post.get("url", "")
                if not post_url:
                    continue
                
                print(f"[API] 답글 수집 중: {i+1}/{len(filtered_posts)} - {post_url}")
                
                try:
                    # 남은 수집 가능 개수 계산
                    remaining = max_total_posts - total_collected
                    
                    thread_data = await scrape_thread_with_replies(
                        post_url=post_url,
                        max_depth=max_reply_depth,
                        max_total_posts=remaining,
                    )

                    # 같은 스레드(같은 root post_id) 중복 제거:
                    # 프로필에는 루트 글과 reply 글이 모두 섞여 나올 수 있어,
                    # reply URL로 요청해도 scrape_thread_with_replies()가 루트로 정규화한 뒤
                    # root post_id 기준으로 1번만 포함한다.
                    root_post_id = thread_data.get("post_id")
                    if root_post_id and root_post_id in seen_root_post_ids:
                        print(f"[API] 중복 스레드 스킵: root post_id={root_post_id} (url={post_url})")
                        continue
                    if root_post_id:
                        seen_root_post_ids.add(root_post_id)
                    
                    # 원본 created_at 유지 (프로필에서 가져온 정확한 날짜)
                    thread_data["created_at"] = post.get("created_at")
                    
                    replies_count = len(thread_data.get("replies", []))
                    thread_total = 1 + replies_count
                    total_collected += thread_total
                    
                    thread_responses.append(ThreadWithRepliesResponse(
                        post_id=thread_data.get("post_id"),
                        text=thread_data.get("text", ""),
                        created_at=thread_data.get("created_at"),
                        url=thread_data.get("url", ""),
                        author=thread_data.get("author"),
                        media=thread_data.get("media", []),
                        replies=convert_replies(thread_data.get("replies", [])),
                        total_replies_count=thread_data.get("total_replies_count", 0),
                        scraped_at=datetime.now(KST).isoformat(),
                    ))
                    
                    print(f"[API] 스레드 수집 완료: root 1개 + 답글 {replies_count}개 (누적 {total_collected}개)")
                    
                except Exception as e:
                    print(f"[API] 답글 수집 실패: {e}")
                    # 답글 수집 실패 시 원본 게시물만 추가
                    thread_responses.append(ThreadWithRepliesResponse(
                        post_id=None,
                        text=post.get("text", ""),
                        created_at=post.get("created_at"),
                        url=post_url,
                        author=None,
                        media=post.get("media", []),
                        replies=[],
                        total_replies_count=0,
                        scraped_at=datetime.now(KST).isoformat(),
                    ))
                    total_collected += 1
                
                # 요청 간 간격 (rate limiting 방지)
                await _asyncio.sleep(2)
            
            print(f"[API] 전체 수집 완료: 총 {total_collected}개 게시물")
            
            # 메타데이터 계산
            total_scrape_duration = round(time.time() - scrape_start_time, 2)
            total_replies_collected = sum(t.total_replies_count for t in thread_responses)
            posts_with_replies_count = sum(1 for t in thread_responses if t.total_replies_count > 0)
            
            # 날짜 범위 계산
            post_dates = [parse_datetime(t.created_at) for t in thread_responses if t.created_at]
            post_dates = [d for d in post_dates if d is not None]
            if post_dates:
                newest_date = max(post_dates)
                oldest_date = min(post_dates)
                date_range_days = (newest_date - oldest_date).days
                newest_date_str = newest_date.isoformat()
                oldest_date_str = oldest_date.isoformat()
            else:
                date_range_days = 0
                newest_date_str = oldest_date_str = None
            
            # 상태 결정
            if len(thread_responses) == 0:
                scrape_status = "failed"
            elif total_collected < max_total_posts and len(filtered_posts) > len(thread_responses):
                scrape_status = "partial"
            else:
                scrape_status = "success"
            
            # 깔끔한 단일 객체 응답
            response = {
                "meta": {
                    "username": username,
                    "scraped_at": datetime.now(KST).isoformat(),
                    "duration_seconds": total_scrape_duration,
                    "scroll_rounds": profile_scroll_rounds,
                    "status": scrape_status,
                },
                "stats": {
                    "total_scraped": total_before_filter,
                    "filtered_count": len(thread_responses),
                    "excluded_count": total_before_filter - len(filtered_posts),
                    "total_replies": total_replies_collected,
                    "posts_with_replies": posts_with_replies_count,
                },
                "date_range": {
                    "days": date_range_days,
                    "newest": newest_date_str,
                    "oldest": oldest_date_str,
                },
                "filter": {
                    "type": filter_desc,
                    "cutoff_date": cutoff_utc.isoformat() if cutoff_utc else None,
                },
                "posts": [t.model_dump() for t in thread_responses],
            }
            
            return JSONResponse(content=response)
        else:
            # 답글 미포함 시
            post_responses = []
            for post in filtered_posts:
                try:
                    post_responses.append({
                        "text": post.get("text") or "",
                        "created_at": post.get("created_at"),
                        "url": post.get("url") or "",
                        "media": post.get("media", []),
                    })
                except Exception as post_error:
                    print(f"PostResponse 변환 실패: {post_error}")
                    continue
            
            # 메타데이터 계산
            total_scrape_duration = round(time.time() - scrape_start_time, 2)
            
            # 날짜 범위 계산
            post_dates = [parse_datetime(p["created_at"]) for p in post_responses if p.get("created_at")]
            post_dates = [d for d in post_dates if d is not None]
            if post_dates:
                newest_date = max(post_dates)
                oldest_date = min(post_dates)
                date_range_days = (newest_date - oldest_date).days
                newest_date_str = newest_date.isoformat()
                oldest_date_str = oldest_date.isoformat()
            else:
                date_range_days = 0
                newest_date_str = oldest_date_str = None
            
            # 상태 결정
            if len(post_responses) == 0:
                scrape_status = "failed" if total_before_filter == 0 else "success"
            else:
                scrape_status = "success"
            
            # 깔끔한 단일 객체 응답
            response = {
                "meta": {
                    "username": username,
                    "scraped_at": datetime.now(KST).isoformat(),
                    "duration_seconds": total_scrape_duration,
                    "scroll_rounds": profile_scroll_rounds,
                    "status": scrape_status,
                },
                "stats": {
                    "total_scraped": total_before_filter,
                    "filtered_count": len(post_responses),
                    "excluded_count": total_before_filter - len(post_responses),
                    "total_replies": 0,
                    "posts_with_replies": 0,
                },
                "date_range": {
                    "days": date_range_days,
                    "newest": newest_date_str,
                    "oldest": oldest_date_str,
                },
                "filter": {
                    "type": filter_desc,
                    "cutoff_date": cutoff_utc.isoformat() if cutoff_utc else None,
                },
                "posts": post_responses,
            }
            
            return JSONResponse(content=response)
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
        
        scrape_result = await scrape_threads_profile(
            username=username,
            max_posts=request.max_posts,
            max_scroll_rounds=200,  # 내부적으로 충분히 높은 값 사용
            cutoff_utc=compute_cutoff_utc(since_days=request.since_days, since_date=request.since_date),
        )
        posts = scrape_result["posts"]
        
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
                    "media": post.get("media", []),
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


@app.post("/v2/scrape")
async def scrape_v2(request: V2ScrapeRequest):
    """새로운 응답 형식의 스크래핑 엔드포인트 (POST 전용)
    
    스타일:
    - threaded: 루트 스레드 단위로 replies 포함
    - flat: 모든 게시물을 평탄화하여 반환
    """
    try:
        username = request.username.lstrip("@")
        if not username:
            raise HTTPException(status_code=400, detail="사용자명이 필요합니다")
        
        # 기존 로직 재사용
        result = await scrape_single_account_v2(
            username=username,
            max_posts=None,
            max_scroll_rounds=200,
            since_days=request.since_days,
            since_date=request.since_date,
            include_replies=request.include_replies,
            max_reply_depth=request.max_reply_depth,
            max_total_posts=request.max_total_posts,
            semaphore=None,
            shared_counter=None,
        )
        
        response = {
            "meta": {
                "username": result["meta"]["username"],
                "scraped_at": result["meta"]["scraped_at"],
                "duration_seconds": result["meta"]["duration_seconds"],
                "scroll_rounds": result["meta"]["scroll_rounds"],
                "status": result["meta"]["status"],
            },
            "filter": {
                "type": result["filter"]["type"],
                "cutoff_date": result["filter"]["cutoff_date"],
            },
            "stats": {
                "total_scraped": result["stats"]["total_scraped"],
                "filtered_count": result["stats"]["filtered_count"],
                "excluded_count": result["stats"]["excluded_count"],
                "total_replies": result["stats"]["total_replies"],
                "posts_with_replies": result["stats"]["posts_with_replies"],
            },
            "request": {
                "since_days": request.since_days,
                "since_date": request.since_date,
                "include_replies": request.include_replies,
                "max_reply_depth": request.max_reply_depth,
                "max_total_posts": request.max_total_posts,
                "response_style": request.response_style,
            },
            "data": build_v2_data_from_result(
                result=result,
                response_style=request.response_style,
                include_replies=request.include_replies,
            ),
        }
        
        return JSONResponse(content=response)
    except HTTPException:
        raise
    except Exception as e:
        error_detail = f"스크래핑 중 오류 발생: {str(e)}"
        print(f"v2 스크래핑 오류 상세:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_detail)

class SharedCounter:
    """스레드 안전한 공유 카운터 (병렬 스크래핑에서 max_total 제어용)"""
    def __init__(self, max_total: int):
        self.max_total = max_total
        self.count = 0
        self._lock = asyncio.Lock()
    
    async def try_add(self, amount: int) -> int:
        """
        지정된 양을 추가 시도하고 실제로 추가된 양을 반환.
        max_total을 초과하면 가능한 양만 추가.
        """
        async with self._lock:
            remaining = self.max_total - self.count
            if remaining <= 0:
                return 0
            added = min(amount, remaining)
            self.count += added
            return added
    
    async def get_remaining(self) -> int:
        """남은 수집 가능 개수 반환"""
        async with self._lock:
            return max(0, self.max_total - self.count)
    
    async def is_full(self) -> bool:
        """max_total에 도달했는지 확인"""
        async with self._lock:
            return self.count >= self.max_total


async def scrape_single_account_v2(
    username: str,
    max_posts: Optional[int],
    max_scroll_rounds: int,
    since_days: Optional[int] = None,
    since_date: Optional[str] = None,
    include_replies: bool = True,
    max_reply_depth: int = 1,
    max_total_posts: int = 100,
    semaphore: Optional[asyncio.Semaphore] = None,
    shared_counter: Optional[SharedCounter] = None,
) -> Dict[str, Any]:
    """단일 계정 스크래핑 헬퍼 함수 (GET /scrape와 동일한 로직)
    
    shared_counter가 제공되면 전체 max_total을 실시간으로 체크하여
    도달 시 조기 종료합니다.
    """
    import time
    start_time = time.time()
    username = username.lstrip("@")
    cutoff_utc = compute_cutoff_utc(since_days=since_days, since_date=since_date)
    
    # 필터 설명 생성
    if since_days:
        filter_desc = f"최근 {since_days}일 이내"
    elif since_date:
        filter_desc = f"{since_date} 이후"
    else:
        filter_desc = None
    
    async def do_scrape():
        # 시작 전에 이미 max_total 도달했는지 확인
        if shared_counter and await shared_counter.is_full():
            print(f"[batch] @{username}: max_total 이미 도달, 스킵")
            return {
                "meta": {
                    "username": username,
                    "scraped_at": datetime.now(KST).isoformat(),
                    "duration_seconds": 0,
                    "scroll_rounds": 0,
                    "status": "skipped",
                    "reason": "max_total reached",
                },
                "stats": {
                    "total_scraped": 0,
                    "filtered_count": 0,
                    "excluded_count": 0,
                    "total_replies": 0,
                    "posts_with_replies": 0,
                },
                "date_range": {"days": 0, "newest": None, "oldest": None},
                "filter": {"type": filter_desc, "cutoff_date": cutoff_utc.isoformat() if cutoff_utc else None},
                "posts": [],
            }
        
        if not username:
            return {
                "meta": {
                    "username": username,
                    "scraped_at": datetime.now(KST).isoformat(),
                    "duration_seconds": 0,
                    "scroll_rounds": 0,
                    "status": "failed",
                    "error": "사용자명이 비어있습니다",
                },
                "stats": {
                    "total_scraped": 0,
                    "filtered_count": 0,
                    "excluded_count": 0,
                    "total_replies": 0,
                    "posts_with_replies": 0,
                },
                "date_range": {"days": 0, "newest": None, "oldest": None},
                "filter": {"type": filter_desc, "cutoff_date": cutoff_utc.isoformat() if cutoff_utc else None},
                "posts": [],
            }
        
        try:
            scrape_result = await scrape_threads_profile(
                username=username,
                max_posts=max_posts,
                max_scroll_rounds=max_scroll_rounds,
                cutoff_utc=cutoff_utc,
            )
            posts = scrape_result["posts"]
            scroll_rounds = scrape_result["scroll_rounds"]
            
            total_before_filter = len(posts)
            
            # 날짜 필터 적용
            filtered_posts = filter_posts_by_date(posts, since_days=since_days, since_date=since_date)
            
            # include_replies에 따라 처리
            if include_replies:
                # 답글 포함 - GET /scrape와 동일한 로직
                post_responses = []
                total_collected = 0
                total_replies_collected = 0
                posts_with_replies_count = 0
                seen_root_post_ids: set = set()
                
                for post in filtered_posts:
                    # 공유 카운터로 max_total 체크
                    if shared_counter and await shared_counter.is_full():
                        print(f"[batch] @{username}: 스크래핑 중 max_total 도달, 조기 종료")
                        break
                    
                    # 로컬 max_total_posts 체크
                    if total_collected >= max_total_posts:
                        break
                    
                    post_url = post.get("url", "")
                    if not post_url:
                        continue
                    
                    try:
                        remaining = max_total_posts - total_collected
                        thread_data = await scrape_thread_with_replies(
                            post_url=post_url,
                            max_depth=max_reply_depth,
                            max_total_posts=remaining,
                        )
                        
                        root_post_id = thread_data.get("post_id")
                        if root_post_id and root_post_id in seen_root_post_ids:
                            continue
                        if root_post_id:
                            seen_root_post_ids.add(root_post_id)
                        
                        thread_data["created_at"] = post.get("created_at")
                        replies_count = len(thread_data.get("replies", []))
                        post_total = 1 + replies_count
                        
                        # 공유 카운터에 추가 시도
                        if shared_counter:
                            added = await shared_counter.try_add(post_total)
                            if added == 0:
                                print(f"[batch] @{username}: 공유 카운터 가득 참, 조기 종료")
                                break
                            # 부분적으로만 추가된 경우도 일단 포함 (이미 수집됨)
                        
                        total_collected += post_total
                        total_replies_collected += replies_count
                        if replies_count > 0:
                            posts_with_replies_count += 1
                        
                        post_responses.append(thread_data)
                        
                    except Exception as e:
                        # 공유 카운터에 1 추가
                        if shared_counter:
                            added = await shared_counter.try_add(1)
                            if added == 0:
                                break
                        
                        post_responses.append({
                            "post_id": None,
                            "text": post.get("text", ""),
                            "created_at": post.get("created_at"),
                            "url": post_url,
                            "author": None,
                            "replies": [],
                            "total_replies_count": 0,
                        })
                        total_collected += 1
                    
                    await asyncio.sleep(1)  # rate limiting
            else:
                # 답글 미포함
                post_responses = []
                total_replies_collected = 0
                posts_with_replies_count = 0
                if any("is_reply" in p for p in filtered_posts):
                    filtered_posts = [p for p in filtered_posts if not p.get("is_reply")]
                
                for post in filtered_posts[:max_total_posts]:
                    # 공유 카운터 체크
                    if shared_counter and await shared_counter.is_full():
                        break
                    
                    try:
                        # 공유 카운터에 1 추가
                        if shared_counter:
                            added = await shared_counter.try_add(1)
                            if added == 0:
                                break
                        
                        post_responses.append({
                            "text": post.get("text") or "",
                            "created_at": post.get("created_at"),
                            "url": post.get("url") or "",
                        })
                    except Exception:
                        continue
            
            duration = round(time.time() - start_time, 2)
            
            # 날짜 범위 계산
            post_dates = [parse_datetime(p["created_at"]) for p in post_responses if p.get("created_at")]
            post_dates = [d for d in post_dates if d is not None]
            if post_dates:
                newest_date = max(post_dates)
                oldest_date = min(post_dates)
                date_range_days = (newest_date - oldest_date).days
                newest_date_str = newest_date.isoformat()
                oldest_date_str = oldest_date.isoformat()
            else:
                date_range_days = 0
                newest_date_str = oldest_date_str = None
            
            # 상태 결정
            if len(post_responses) == 0:
                status = "failed" if total_before_filter == 0 else "success"
            else:
                status = "success"
            
            return {
                "meta": {
                    "username": username,
                    "scraped_at": datetime.now(KST).isoformat(),
                    "duration_seconds": duration,
                    "scroll_rounds": scroll_rounds,
                    "status": status,
                },
                "stats": {
                    "total_scraped": total_before_filter,
                    "filtered_count": len(post_responses),
                    "excluded_count": total_before_filter - len(filtered_posts),
                    "total_replies": total_replies_collected,
                    "posts_with_replies": posts_with_replies_count,
                },
                "date_range": {
                    "days": date_range_days,
                    "newest": newest_date_str,
                    "oldest": oldest_date_str,
                },
                "filter": {
                    "type": filter_desc,
                    "cutoff_date": cutoff_utc.isoformat() if cutoff_utc else None,
                },
                "posts": post_responses,
            }
        except Exception as e:
            duration = round(time.time() - start_time, 2)
            return {
                "meta": {
                    "username": username,
                    "scraped_at": datetime.now(KST).isoformat(),
                    "duration_seconds": duration,
                    "scroll_rounds": 0,
                    "status": "failed",
                    "error": str(e),
                },
                "stats": {
                    "total_scraped": 0,
                    "filtered_count": 0,
                    "excluded_count": 0,
                    "total_replies": 0,
                    "posts_with_replies": 0,
                },
                "date_range": {"days": 0, "newest": None, "oldest": None},
                "filter": {"type": filter_desc, "cutoff_date": cutoff_utc.isoformat() if cutoff_utc else None},
                "posts": [],
            }
    
    # Semaphore로 동시성 제한
    if semaphore:
        async with semaphore:
            print(f"[batch] 스크래핑 시작: @{username}")
            result = await do_scrape()
            print(f"[batch] 스크래핑 완료: @{username} ({result['meta']['duration_seconds']}초, {result['stats']['filtered_count']}개)")
            return result
    else:
        return await do_scrape()


async def scrape_single_account(
    username: str,
    max_posts: Optional[int],
    max_scroll_rounds: int,
    since_days: Optional[int] = None,
    since_date: Optional[str] = None,
) -> BatchScrapeItem:
    """단일 계정 스크래핑 헬퍼 함수 (레거시 호환용)"""
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
        
        scrape_result = await scrape_threads_profile(
            username=username,
            max_posts=max_posts,
            max_scroll_rounds=max_scroll_rounds,
            cutoff_utc=compute_cutoff_utc(since_days=since_days, since_date=since_date),
        )
        posts = scrape_result["posts"]
        
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
                    "media": post.get("media", []),
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
async def batch_scrape_legacy(request: BatchScrapeRequest):
    """배치 스크래핑 요청 (레거시 호환용)"""
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


@app.post("/batch-scrape")
async def batch_scrape_v2(request: BatchScrapeRequestV2):
    """배치 스크래핑 - 모든 계정 병렬 처리 + 전체 max_total 실시간 제어
    
    여러 계정을 한 번에 스크래핑합니다.
    - 모든 계정을 동시에 병렬 처리 (최대 5개 동시 실행)
    - max_per_account: 계정별 최대 스크랩 갯수
    - max_total: 전체 최대 갯수 - 실시간으로 체크하여 도달 시 조기 종료
    
    파라미터:
    - usernames: 사용자명 리스트 (필수)
    - since_days: 최근 N일 이내 (기본: 1일)
    - max_per_account: 계정별 최대 스크랩 갯수 (기본: 10)
    - max_total: 전체 최대 스크랩 갯수 (기본: 300, 실시간 체크)
    - include_replies: 답글 포함 여부 (기본: True)
    - max_reply_depth: 답글 깊이 (기본: 1)
    
    예시:
    ```json
    {
        "usernames": ["zuck", "meta", "instagram"],
        "max_per_account": 10,
        "max_total": 300
    }
    ```
    """
    import time
    batch_start_time = time.time()
    
    try:
        if not request.usernames:
            raise HTTPException(status_code=400, detail="사용자명 리스트가 비어있습니다")
        
        unique_usernames = list(dict.fromkeys(request.usernames))
        
        print(f"[batch] 🚀 병렬 배치 스크래핑 시작: {len(unique_usernames)}개 계정")
        print(f"[batch] 설정: max_per_account={request.max_per_account}, max_total={request.max_total}, 동시처리=5")
        
        # 공유 카운터 생성 (max_total 실시간 제어용)
        shared_counter = SharedCounter(max_total=request.max_total)
        
        # 세마포어로 동시성 5개 제한
        semaphore = asyncio.Semaphore(5)
        
        # 모든 계정에 대해 병렬 태스크 생성
        tasks = [
            scrape_single_account_v2(
                username=username,
                max_posts=request.max_per_account,
                max_scroll_rounds=50,
                since_days=request.since_days,
                since_date=request.since_date,
                include_replies=request.include_replies,
                max_reply_depth=request.max_reply_depth,
                max_total_posts=request.max_per_account,
                semaphore=semaphore,
                shared_counter=shared_counter,
            )
            for username in unique_usernames
        ]
        
        # 모든 태스크 병렬 실행
        results = await asyncio.gather(*tasks)
        
        # 전체 메타데이터 계산
        batch_duration = round(time.time() - batch_start_time, 2)
        success_count = sum(1 for r in results if r["meta"]["status"] == "success")
        skipped_count = sum(1 for r in results if r["meta"]["status"] == "skipped")
        failed_count = sum(1 for r in results if r["meta"]["status"] == "failed")
        total_scraped = sum(r["stats"]["total_scraped"] for r in results)
        total_filtered = sum(r["stats"]["filtered_count"] for r in results)
        total_excluded = sum(r["stats"]["excluded_count"] for r in results)
        total_replies = sum(r["stats"]["total_replies"] for r in results)
        
        # 스킵된 계정 목록
        skipped_usernames = [r["meta"]["username"] for r in results if r["meta"]["status"] == "skipped"]
        
        # 전체 날짜 범위 계산
        all_dates = []
        for r in results:
            if r["date_range"]["newest"]:
                all_dates.append(parse_datetime(r["date_range"]["newest"]))
            if r["date_range"]["oldest"]:
                all_dates.append(parse_datetime(r["date_range"]["oldest"]))
        all_dates = [d for d in all_dates if d is not None]
        
        if all_dates:
            overall_newest = max(all_dates).isoformat()
            overall_oldest = min(all_dates).isoformat()
            overall_days = (max(all_dates) - min(all_dates)).days
        else:
            overall_newest = overall_oldest = None
            overall_days = 0
        
        print(f"[batch] ✅ 병렬 배치 스크래핑 완료!")
        print(f"[batch]    성공: {success_count}, 스킵: {skipped_count}, 실패: {failed_count}")
        print(f"[batch]    총 수집: {total_filtered}개, 소요시간: {batch_duration}초")
        
        response = {
            "total_meta": {
                "accounts_requested": len(unique_usernames),
                "accounts_processed": len(results),
                "accounts_skipped": skipped_count,
                "skipped_usernames": skipped_usernames,
                "success_count": success_count,
                "failed_count": failed_count,
                "total_duration_seconds": batch_duration,
                "total_collected": total_filtered,
                "max_total_reached": shared_counter.count >= request.max_total,
                "total_replies": total_replies,
                "settings": {
                    "max_per_account": request.max_per_account,
                    "max_total": request.max_total,
                    "since_days": request.since_days,
                    "include_replies": request.include_replies,
                    "concurrency": 5,
                },
                "date_range": {
                    "days": overall_days,
                    "newest": overall_newest,
                    "oldest": overall_oldest,
                },
                "completed_at": datetime.now(KST).isoformat(),
            },
            "results": results,
        }
        
        return JSONResponse(content=response)
    except HTTPException:
        raise
    except Exception as e:
        print(f"배치 스크래핑 오류:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"배치 스크래핑 중 오류 발생: {str(e)}")


@app.post("/v2/batch-scrape")
async def batch_scrape_v2_clean(request: V2BatchScrapeRequest):
    """새로운 응답 형식의 배치 스크래핑 엔드포인트 (POST 전용)"""
    import time
    batch_start_time = time.time()
    
    try:
        if not request.usernames:
            raise HTTPException(status_code=400, detail="사용자명 리스트가 비어있습니다")
        
        unique_usernames = list(dict.fromkeys(request.usernames))
        
        shared_counter = SharedCounter(max_total=request.max_total)
        semaphore = asyncio.Semaphore(5)
        
        tasks = [
            scrape_single_account_v2(
                username=username,
                max_posts=request.max_per_account,
                max_scroll_rounds=50,
                since_days=request.since_days,
                since_date=request.since_date,
                include_replies=request.include_replies,
                max_reply_depth=request.max_reply_depth,
                max_total_posts=request.max_per_account,
                semaphore=semaphore,
                shared_counter=shared_counter,
            )
            for username in unique_usernames
        ]
        
        results = await asyncio.gather(*tasks)
        
        batch_duration = round(time.time() - batch_start_time, 2)
        success_count = sum(1 for r in results if r["meta"]["status"] == "success")
        skipped_count = sum(1 for r in results if r["meta"]["status"] == "skipped")
        failed_count = sum(1 for r in results if r["meta"]["status"] == "failed")
        total_filtered = sum(r["stats"]["filtered_count"] for r in results)
        total_replies = sum(r["stats"]["total_replies"] for r in results)
        
        skipped_usernames = [r["meta"]["username"] for r in results if r["meta"]["status"] == "skipped"]
        
        # 전체 날짜 범위 계산
        all_dates = []
        for r in results:
            if r["date_range"]["newest"]:
                all_dates.append(parse_datetime(r["date_range"]["newest"]))
            if r["date_range"]["oldest"]:
                all_dates.append(parse_datetime(r["date_range"]["oldest"]))
        all_dates = [d for d in all_dates if d is not None]
        
        if all_dates:
            overall_newest = max(all_dates).isoformat()
            overall_oldest = min(all_dates).isoformat()
            overall_days = (max(all_dates) - min(all_dates)).days
        else:
            overall_newest = overall_oldest = None
            overall_days = 0
        
        cleaned_results = []
        for r in results:
            cleaned_results.append({
                "meta": r["meta"],
                "filter": r["filter"],
                "stats": r["stats"],
                "data": build_v2_data_from_result(
                    result=r,
                    response_style=request.response_style,
                    include_replies=request.include_replies,
                ),
            })
        
        response = {
            "batch_meta": {
                "accounts_requested": len(unique_usernames),
                "accounts_processed": len(results),
                "accounts_skipped": skipped_count,
                "skipped_usernames": skipped_usernames,
                "success_count": success_count,
                "failed_count": failed_count,
                "total_duration_seconds": batch_duration,
                "total_collected": total_filtered,
                "max_total_reached": shared_counter.count >= request.max_total,
                "total_replies": total_replies,
                "settings": {
                    "max_per_account": request.max_per_account,
                    "max_total": request.max_total,
                    "since_days": request.since_days,
                    "include_replies": request.include_replies,
                    "concurrency": 5,
                    "response_style": request.response_style,
                },
                "date_range": {
                    "days": overall_days,
                    "newest": overall_newest,
                    "oldest": overall_oldest,
                },
                "completed_at": datetime.now(KST).isoformat(),
            },
            "request": {
                "since_days": request.since_days,
                "since_date": request.since_date,
                "include_replies": request.include_replies,
                "max_reply_depth": request.max_reply_depth,
                "max_per_account": request.max_per_account,
                "max_total": request.max_total,
                "response_style": request.response_style,
            },
            "results": cleaned_results,
        }
        
        return JSONResponse(content=response)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"v2 배치 스크래핑 중 오류 발생: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.getenv("PORT", "8001"))
    print(f"서버 시작 중... http://0.0.0.0:{port}")
    print(f"API 문서: http://localhost:{port}/docs")
    uvicorn.run(app, host="0.0.0.0", port=port)

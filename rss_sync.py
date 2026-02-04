"""Account-based scheduled scraper with concurrency=5 and SQLite persistence."""
import asyncio
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from dateutil import parser as date_parser

from scraper import scrape_threads_profile


DB_PATH = os.environ.get("RSS_DB_PATH", os.path.join("db", "rss_cache.db"))
SCHEMA_PATH = os.environ.get("RSS_SCHEMA_PATH", os.path.join("db", "schema.sql"))
SCRAPE_CONCURRENCY = int(os.environ.get("RSS_SCRAPE_CONCURRENCY", "5"))
LOG_PATH = os.environ.get("RSS_SYNC_LOG_PATH", os.path.join("db", "rss_sync.log"))
# Safety window to avoid missing posts around cutoff
CUTOFF_SAFETY_MINUTES = int(os.environ.get("RSS_CUTOFF_SAFETY_MINUTES", "120"))


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    line = f"[{ts}] {msg}\n"
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = date_parser.parse(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def ensure_schema(conn: sqlite3.Connection) -> None:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        conn.executescript(f.read())

    # Backward-compatible migrations
    cols = {r[1] for r in conn.execute("PRAGMA table_info(feed_sources)").fetchall()}
    if "is_active" not in cols:
        conn.execute("ALTER TABLE feed_sources ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")

    # Ensure rss_schedule row exists
    rows = conn.execute("SELECT id FROM rss_schedule WHERE id = 1").fetchall()
    if not rows:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO rss_schedule (id, is_active, interval_minutes, start_time, last_run_at, updated_at) "
            "VALUES (1, 1, 30, '09:00', NULL, ?)",
            (now,),
        )
    cols = {r[1] for r in conn.execute("PRAGMA table_info(rss_schedule)").fetchall()}
    if "start_time" not in cols:
        conn.execute("ALTER TABLE rss_schedule ADD COLUMN start_time TEXT NOT NULL DEFAULT '09:00'")

    rows = conn.execute("SELECT id FROM rss_rate_limits WHERE id = 1").fetchall()
    if not rows:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO rss_rate_limits (id, window_seconds, max_requests, updated_at) "
            "VALUES (1, 300, 60, ?)",
            (now,),
        )
    cols = {r[1] for r in conn.execute("PRAGMA table_info(rss_feed_cache)").fetchall()}
    # If legacy cache with `id` exists, drop/recreate (cache can be rebuilt)
    if "id" in cols:
        conn.execute("DROP TABLE IF EXISTS rss_feed_cache")
        conn.executescript(
            """
        CREATE TABLE IF NOT EXISTS rss_feed_cache (
          username TEXT NOT NULL,
          limit_count INTEGER NOT NULL,
          etag TEXT NOT NULL,
          last_modified TEXT,
          xml TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (username, limit_count)
        );
        """
        )


def load_accounts(conn: sqlite3.Connection, only_active: bool = True) -> List[Dict[str, Any]]:
    if only_active:
        rows = conn.execute(
            "SELECT id, username FROM feed_sources WHERE is_active = 1 ORDER BY id ASC"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, username FROM feed_sources ORDER BY id ASC"
        ).fetchall()
    return [{"id": r[0], "username": r[1]} for r in rows]


def get_latest_created_at(conn: sqlite3.Connection, source_id: int) -> Optional[datetime]:
    row = conn.execute(
        "SELECT MAX(created_at) FROM posts WHERE source_id = ?",
        (source_id,),
    ).fetchone()
    return _parse_dt(row[0]) if row and row[0] else None


def insert_posts(
    conn: sqlite3.Connection,
    source_id: int,
    posts: List[Dict[str, Any]],
) -> int:
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for p in posts:
        payload.append(
            (
                source_id,
                p.get("post_id"),
                p.get("url"),
                p.get("text"),
                p.get("created_at"),
                now,
                1 if p.get("is_reply") else 0,
                None,
            )
        )
    if not payload:
        return 0
    conn.executemany(
        """
        INSERT OR IGNORE INTO posts
        (source_id, post_id, url, text, created_at, scraped_at, is_reply, parent_post_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return conn.total_changes


async def scrape_one(
    semaphore: asyncio.Semaphore,
    username: str,
    cutoff_utc: Optional[datetime],
) -> List[Dict[str, Any]]:
    async with semaphore:
        result = await scrape_threads_profile(
            username=username,
            max_posts=None,
            max_scroll_rounds=50,
            cutoff_utc=cutoff_utc,
        )
        return result.get("posts", [])


async def run_once(usernames: Optional[List[str]] = None) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        ensure_schema(conn)

        accounts = load_accounts(conn, only_active=True)
        if usernames:
            usernames_set = {u.lstrip("@") for u in usernames}
            accounts = [a for a in accounts if a["username"] in usernames_set]
        if not accounts:
            print("[rss_sync] No accounts in feed_sources. Nothing to do.")
            _log("No accounts to scrape.")
            return

        semaphore = asyncio.Semaphore(SCRAPE_CONCURRENCY)
        tasks = []
        for acc in accounts:
            latest = get_latest_created_at(conn, acc["id"])
            cutoff = None
            if latest:
                cutoff = latest - timedelta(minutes=CUTOFF_SAFETY_MINUTES)
            tasks.append(
                scrape_one(semaphore, acc["username"], cutoff)
            )

        _log(f"Start scrape: {len(accounts)} accounts, concurrency={SCRAPE_CONCURRENCY}")
        results = await asyncio.gather(*tasks)

        # Write sequentially to avoid SQLite write contention
        total_inserted = 0
        for acc, posts in zip(accounts, results):
            with conn:
                total_inserted += insert_posts(conn, acc["id"], posts)
        print(f"[rss_sync] Done. Inserted/ignored changes: {total_inserted}")
        _log(f"Done. Inserted/ignored changes: {total_inserted}")
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(run_once())

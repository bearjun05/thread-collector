"""Account-based scheduled scraper with concurrency=5 and SQLite persistence."""
import asyncio
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from dateutil import parser as date_parser
from email.utils import format_datetime

from scraper import scrape_threads_profile_with_replies
from curation import (
    ensure_schema as ensure_curation_schema,
    run_curation_once as curation_run_once,
    maybe_auto_publish_due as curation_maybe_auto_publish_due,
    refresh_cache as refresh_curated_cache,
    ITEM_FEED as CURATED_ITEM_FEED,
    DIGEST_FEED as CURATED_DIGEST_FEED,
)
from rss_render import (
    xml_escape as _xml_escape,
    build_description_html as _shared_build_description_html,
    build_content_html as _shared_build_content_html,
    collect_youtube_embeds as _shared_collect_youtube_embeds,
    reply_texts_from_dicts as _reply_texts_from_dicts,
    build_enclosures as _build_enclosures,
    build_media_contents as _build_media_contents,
    build_media_players as _build_media_players,
)

DB_PATH = os.environ.get("RSS_DB_PATH", os.path.join("db", "rss_cache.db"))
SCHEMA_PATH = os.environ.get("RSS_SCHEMA_PATH", os.path.join("db", "schema.sql"))
SCRAPE_CONCURRENCY = int(os.environ.get("RSS_SCRAPE_CONCURRENCY", "5"))
LOG_PATH = os.environ.get("RSS_SYNC_LOG_PATH", os.path.join("db", "rss_sync.log"))
# Safety window to avoid missing posts around cutoff
CUTOFF_SAFETY_MINUTES = int(os.environ.get("RSS_CUTOFF_SAFETY_MINUTES", "120"))
RSS_CACHE_LIMITS = os.environ.get("RSS_CACHE_LIMITS", "50")
SCRAPE_WINDOW_HOURS = int(os.environ.get("RSS_SCRAPE_WINDOW_HOURS", "24"))
KST = timezone(timedelta(hours=9))


def _log(msg: str) -> None:
    ts = datetime.now(KST).isoformat()
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
    if "include_replies" not in cols:
        conn.execute("ALTER TABLE feed_sources ADD COLUMN include_replies INTEGER NOT NULL DEFAULT 1")
    if "max_reply_depth" not in cols:
        conn.execute("ALTER TABLE feed_sources ADD COLUMN max_reply_depth INTEGER NOT NULL DEFAULT 1")

    cols = {r[1] for r in conn.execute("PRAGMA table_info(posts)").fetchall()}
    if "media_json" not in cols:
        conn.execute("ALTER TABLE posts ADD COLUMN media_json TEXT")

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
    rows = conn.execute("SELECT id FROM rss_cache_policy WHERE id = 1").fetchall()
    if not rows:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO rss_cache_policy (id, enabled, ttl_seconds, updated_at) "
            "VALUES (1, 1, 300, ?)",
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

    # Curated RSS tables
    ensure_curation_schema(conn)


def _collect_youtube_embeds(root_text: str, replies: List[Dict[str, Any]]) -> List[str]:
    return _shared_collect_youtube_embeds(root_text, _reply_texts_from_dicts(replies))


def _build_description_html(root_text: str, replies: List[Dict[str, Any]]) -> str:
    return _shared_build_description_html(root_text, _reply_texts_from_dicts(replies))

def _build_content_html(
    root_text: str,
    replies: List[Dict[str, Any]],
    root_media_urls: List[str],
    media_urls: List[str],
    youtube_embeds: List[str],
) -> str:
    content_blocks = [{"text": root_text, "media_urls": root_media_urls}]
    for reply in replies or []:
        reply_media = reply.get("media") or []
        reply_media_urls = [m.get("url") for m in reply_media if m.get("url")]
        content_blocks.append(
            {
                "text": reply.get("text") or "",
                "media_urls": reply_media_urls,
            }
        )
    return _shared_build_content_html(
        root_text,
        _reply_texts_from_dicts(replies),
        media_urls,
        youtube_embeds,
        content_blocks=content_blocks,
    )


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


def _parse_cache_limits() -> List[int]:
    limits = []
    for part in RSS_CACHE_LIMITS.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            val = int(part)
        except ValueError:
            continue
        if 1 <= val <= 500:
            limits.append(val)
    return sorted(set(limits)) or [50]


def _build_rss_xml(username: str, rows: List[Dict[str, Any]]) -> tuple[str, str, Optional[str]]:
    channel_title = f"{username} Threads Feed"
    channel_link = f"https://www.threads.com/@{username}"
    channel_desc = f"Threads posts scraped for @{username}"

    items = []
    last_modified = None
    if rows:
        try:
            dt = _parse_dt(rows[0]["created_at"])
            if dt:
                last_modified = format_datetime(dt)
        except Exception:
            last_modified = None

    for r in rows:
        post_id = r.get("post_id")
        url = r.get("url")
        root_text = r.get("text") or ""
        created_at = r.get("created_at")
        replies = r.get("replies", [])
        media = r.get("media", [])
        desc_html = _build_description_html(root_text, replies)
        media_urls = []
        root_media_urls = []
        if media:
            root_media_urls = [m.get("url") for m in media if m.get("url")]
            media_urls.extend(root_media_urls)
        if replies:
            for rep in replies:
                rep_media = rep.get("media", [])
                if rep_media:
                    media_urls.extend([m.get("url") for m in rep_media if m.get("url")])
        # dedupe
        seen_media = set()
        media_urls = [u for u in media_urls if u and not (u in seen_media or seen_media.add(u))]
        created_dt = _parse_dt(created_at) if created_at else None
        pub_date = format_datetime(created_dt) if created_dt else format_datetime(datetime.now(timezone.utc))
        title_source = root_text.strip()
        if not title_source and replies:
            for rep in replies:
                rep_text = (rep.get("text") or "").strip()
                if rep_text:
                    title_source = rep_text
                    break
        title = (title_source or "(no title)").split("\n")[0][:80]
        enclosures = _build_enclosures(media_urls)
        media_contents = _build_media_contents(media_urls)
        youtube_embeds = _collect_youtube_embeds(root_text, replies)
        media_players = _build_media_players(youtube_embeds)
        content_html = _build_content_html(root_text, replies, root_media_urls, media_urls, youtube_embeds)
        items.append(
            f"<item>"
            f"<title>{_xml_escape(title)}</title>"
            f"<link>{_xml_escape(url)}</link>"
            f"<guid>{_xml_escape(post_id or url)}</guid>"
            f"<pubDate>{pub_date}</pubDate>"
            f"<description>{desc_html}</description>"
            f"<content:encoded><![CDATA[{content_html}]]></content:encoded>"
            f"{enclosures}"
            f"{media_contents}"
            f"{media_players}"
            f"</item>"
        )

    xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<?xml-stylesheet type=\"text/xsl\" href=\"/rss.xsl?v=2\"?>"
        "<rss version=\"2.0\" xmlns:content=\"http://purl.org/rss/1.0/modules/content/\" xmlns:media=\"http://search.yahoo.com/mrss/\">"
        "<channel>"
        f"<title>{_xml_escape(channel_title)}</title>"
        f"<link>{_xml_escape(channel_link)}</link>"
        f"<description>{_xml_escape(channel_desc)}</description>"
        + "".join(items)
        + "</channel></rss>"
    )
    etag = hashlib.sha256(xml.encode("utf-8")).hexdigest()
    return xml, etag, last_modified


def _parse_media_json(raw: Optional[str]) -> List[Dict[str, Any]]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def _fetch_roots_with_replies(conn: sqlite3.Connection, source_id: int, limit: int) -> List[Dict[str, Any]]:
    roots = conn.execute(
        "SELECT post_id, url, text, media_json, created_at FROM posts "
        "WHERE source_id = ? AND is_reply = 0 ORDER BY created_at DESC LIMIT ?",
        (source_id, limit),
    ).fetchall()
    results = []
    for r in roots:
        post_id, url, text, media_json, created_at = r
        replies = conn.execute(
            "SELECT post_id, url, text, media_json, created_at FROM posts "
            "WHERE source_id = ? AND is_reply = 1 AND parent_post_id = ? "
            "ORDER BY created_at ASC",
            (source_id, post_id),
        ).fetchall()
        results.append(
            {
                "post_id": post_id,
                "url": url,
                "text": text,
                "media": _parse_media_json(media_json),
                "created_at": created_at,
                "replies": [
                    {
                        "post_id": rr[0],
                        "url": rr[1],
                        "text": rr[2],
                        "media": _parse_media_json(rr[3]),
                        "created_at": rr[4],
                    }
                    for rr in replies
                ],
            }
        )
    return results


def refresh_rss_cache_for_account(conn: sqlite3.Connection, source_id: int, username: str) -> None:
    policy = _get_cache_policy(conn)
    if not policy["enabled"]:
        return
    limits = _parse_cache_limits()
    for limit in limits:
        rows = _fetch_roots_with_replies(conn, source_id, limit)
        xml, etag, last_modified = _build_rss_xml(username, rows)
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO rss_feed_cache (username, limit_count, etag, last_modified, xml, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (username, limit, etag, last_modified, xml, now),
        )
    conn.commit()


def load_accounts(conn: sqlite3.Connection, only_active: bool = True) -> List[Dict[str, Any]]:
    if only_active:
        rows = conn.execute(
            "SELECT id, username, include_replies, max_reply_depth "
            "FROM feed_sources WHERE is_active = 1 ORDER BY id ASC"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, username, include_replies, max_reply_depth "
            "FROM feed_sources ORDER BY id ASC"
        ).fetchall()
    return [
        {
            "id": r[0],
            "username": r[1],
            "include_replies": bool(r[2]) if r[2] is not None else True,
            "max_reply_depth": r[3] if r[3] is not None else 1,
        }
        for r in rows
    ]


def get_latest_created_at(conn: sqlite3.Connection, source_id: int) -> Optional[datetime]:
    row = conn.execute(
        "SELECT MAX(created_at) FROM posts WHERE source_id = ?",
        (source_id,),
    ).fetchone()
    return _parse_dt(row[0]) if row and row[0] else None


def _extract_post_id(post: Dict[str, Any]) -> Optional[str]:
    pid = post.get("post_id")
    if pid:
        return pid
    url = post.get("url") or ""
    if "/post/" in url:
        return url.split("/post/")[-1].split("?")[0]
    return url or None


def _flatten_posts(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flat = []
    for p in posts:
        root_id = _extract_post_id(p) or p.get("url")
        flat.append(
            {
                "post_id": root_id,
                "url": p.get("url"),
                "text": p.get("text"),
                "created_at": p.get("created_at"),
                "is_reply": False,
                "parent_post_id": None,
                "media": p.get("media", []),
            }
        )
        for rep in p.get("replies", []) or []:
            reply_id = _extract_post_id(rep) or rep.get("url")
            flat.append(
                {
                    "post_id": reply_id,
                    "url": rep.get("url"),
                    "text": rep.get("text"),
                    "created_at": rep.get("created_at"),
                    "is_reply": True,
                    "parent_post_id": root_id,
                    "media": rep.get("media", []),
                }
            )
    return flat


def insert_posts(
    conn: sqlite3.Connection,
    source_id: int,
    posts: List[Dict[str, Any]],
) -> int:
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for p in _flatten_posts(posts):
        media_json = None
        if p.get("media"):
            try:
                media_json = json.dumps(p.get("media", []), ensure_ascii=False)
            except Exception:
                media_json = None
        payload.append(
            (
                source_id,
                p.get("post_id"),
                p.get("url"),
                p.get("text"),
                media_json,
                p.get("created_at"),
                now,
                1 if p.get("is_reply") else 0,
                p.get("parent_post_id"),
            )
        )
    if not payload:
        return 0
    conn.executemany(
        """
        INSERT INTO posts
        (source_id, post_id, url, text, media_json, created_at, scraped_at, is_reply, parent_post_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(post_id) DO UPDATE SET
          source_id=excluded.source_id,
          url=excluded.url,
          text=excluded.text,
          media_json=excluded.media_json,
          created_at=excluded.created_at,
          scraped_at=excluded.scraped_at,
          is_reply=excluded.is_reply,
          parent_post_id=excluded.parent_post_id
        """,
        payload,
    )
    return conn.total_changes


async def scrape_one(
    semaphore: asyncio.Semaphore,
    username: str,
    cutoff_utc: Optional[datetime],
    include_replies: bool,
    max_reply_depth: int,
    max_total_posts: Optional[int],
) -> List[Dict[str, Any]]:
    async with semaphore:
        result = await scrape_threads_profile_with_replies(
            username=username,
            max_posts=None,
            max_scroll_rounds=50,
            include_replies=include_replies,
            max_reply_depth=max_reply_depth,
            max_total_posts=max_total_posts or 300,
            cutoff_utc=cutoff_utc,
        )
        return result or []


async def run_once(
    usernames: Optional[List[str]] = None,
    max_total_posts: Optional[int] = None,
    trigger_curation: bool = True,
) -> None:
    started_at = datetime.now(timezone.utc)
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
        cutoff = datetime.now(timezone.utc) - timedelta(hours=SCRAPE_WINDOW_HOURS)
        for acc in accounts:
            tasks.append(
                scrape_one(
                    semaphore,
                    acc["username"],
                    cutoff,
                    acc.get("include_replies", True),
                    acc.get("max_reply_depth", 1),
                    max_total_posts,
                )
            )

        extra = f", max_total_posts={max_total_posts}" if max_total_posts else ""
        _log(
            f"Start scrape: {len(accounts)} accounts, concurrency={SCRAPE_CONCURRENCY}, "
            f"window_hours={SCRAPE_WINDOW_HOURS}{extra}"
        )
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Write sequentially to avoid SQLite write contention
        total_inserted = 0
        total_posts = 0
        success_accounts = 0
        failed_accounts = 0
        failures = []
        for acc, res in zip(accounts, results):
            if isinstance(res, Exception):
                failed_accounts += 1
                failures.append(f"@{acc['username']}: {type(res).__name__}")
                continue
            posts = res or []
            total_posts += len(posts)
            success_accounts += 1
            with conn:
                total_inserted += insert_posts(conn, acc["id"], posts)
            # refresh cache after each account scrape
            try:
                refresh_rss_cache_for_account(conn, acc["id"], acc["username"])
            except Exception:
                pass

        if trigger_curation:
            try:
                # Trigger curation immediately after scrape run. run_curation_once handles
                # same-day deduplication and will skip when already generated.
                run_result = curation_run_once(conn, force=False, scraped_since=started_at, logger=_log)
                if isinstance(run_result, dict):
                    _log(
                        "[curation] run_once result "
                        f"status={run_result.get('status')} "
                        f"reason={run_result.get('reason')} "
                        f"run_id={run_result.get('run_id')}"
                    )
                pub = curation_maybe_auto_publish_due(conn, logger=_log)
                if pub:
                    refresh_curated_cache(conn, CURATED_ITEM_FEED, limit=10)
                    refresh_curated_cache(conn, CURATED_DIGEST_FEED, limit=10)
                    _log("[curation] refreshed curated RSS cache after publish")
                    if isinstance(pub, dict):
                        _log(
                            "[curation] auto publish result "
                            f"publication_id={pub.get('publication_id')} "
                            f"item_count={pub.get('item_count')}"
                        )
                else:
                    _log("[curation] auto publish skipped (no due run)")
            except Exception as curation_err:
                _log(f"[curation] scheduler hook failed: {type(curation_err).__name__}: {curation_err}")
        else:
            _log("[curation] scheduler hook skipped (trigger_curation=false)")
        duration = int((datetime.now(timezone.utc) - started_at).total_seconds())
        summary = (
            f"Done. accounts_total={len(accounts)} success={success_accounts} failed={failed_accounts} "
            f"posts_scraped={total_posts} inserted_or_ignored={total_inserted} duration_sec={duration}"
        )
        if failures:
            summary += f" failures=[{'; '.join(failures)}]"
        print(f"[rss_sync] {summary}")
        _log(summary)
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(run_once())

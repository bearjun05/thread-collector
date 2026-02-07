"""Curated RSS pipeline and rendering.

This module is intentionally self-contained so app.py and rss_sync.py can call
it without circular dependencies.
"""

import json
import hashlib
import os
import urllib.request
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from dateutil import parser as date_parser

from rss_render import (
    xml_escape,
    build_content_html,
    build_description_html,
    build_enclosures,
    build_media_contents,
    build_media_players,
    collect_youtube_embeds,
)

UTC = timezone.utc
KST = timezone(timedelta(hours=9))

FEED_KEY = "ai-daily"
ITEM_FEED = "item"
DIGEST_FEED = "digest"

DEFAULT_CURATION_CONFIG: Dict[str, Any] = {
    "candidate_generation_enabled": 1,
    "auto_publish_enabled": 1,
    "rss_public_enabled": 1,
    "target_daily_spend_usd": 1.0,  # soft target only
    "daily_run_time_kst": "06:00",
    "publish_delay_minutes": 60,
    "feed_size": 10,
    "window_hours": 24,
    "candidate_pool_size": 20,
    "w_impact": 0.35,
    "w_novelty": 0.25,
    "w_utility": 0.25,
    "w_accessibility": 0.15,
    "recommend_threshold": 75,
    "min_utility": 60,
    "min_accessibility": 55,
    "pick_easy": 6,
    "pick_deep": 2,
    "pick_action": 2,
    "openrouter_model": "openai/gpt-4o-mini",
    "openrouter_api_key": None,
    # soft pricing for visibility only; user can tune in admin
    "input_cost_per_1m": 0.15,
    "output_cost_per_1m": 0.60,
    "prompt_version": "v1",
    "llm_enabled": 1,
    "updated_at": "",
}


def ensure_schema(conn) -> None:
    now = datetime.now(UTC).isoformat()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS curation_config (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          candidate_generation_enabled INTEGER NOT NULL DEFAULT 1,
          auto_publish_enabled INTEGER NOT NULL DEFAULT 1,
          rss_public_enabled INTEGER NOT NULL DEFAULT 1,
          target_daily_spend_usd REAL NOT NULL DEFAULT 1.0,
          daily_run_time_kst TEXT NOT NULL DEFAULT '06:00',
          publish_delay_minutes INTEGER NOT NULL DEFAULT 60,
          feed_size INTEGER NOT NULL DEFAULT 10,
          window_hours INTEGER NOT NULL DEFAULT 24,
          candidate_pool_size INTEGER NOT NULL DEFAULT 20,
          w_impact REAL NOT NULL DEFAULT 0.35,
          w_novelty REAL NOT NULL DEFAULT 0.25,
          w_utility REAL NOT NULL DEFAULT 0.25,
          w_accessibility REAL NOT NULL DEFAULT 0.15,
          recommend_threshold INTEGER NOT NULL DEFAULT 75,
          min_utility INTEGER NOT NULL DEFAULT 60,
          min_accessibility INTEGER NOT NULL DEFAULT 55,
          pick_easy INTEGER NOT NULL DEFAULT 6,
          pick_deep INTEGER NOT NULL DEFAULT 2,
          pick_action INTEGER NOT NULL DEFAULT 2,
          openrouter_model TEXT NOT NULL DEFAULT 'openai/gpt-4o-mini',
          openrouter_api_key TEXT,
          input_cost_per_1m REAL NOT NULL DEFAULT 0.15,
          output_cost_per_1m REAL NOT NULL DEFAULT 0.60,
          prompt_version TEXT NOT NULL DEFAULT 'v1',
          llm_enabled INTEGER NOT NULL DEFAULT 1,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS curation_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_date_kst TEXT NOT NULL UNIQUE,
          status TEXT NOT NULL,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          total_posts INTEGER NOT NULL DEFAULT 0,
          rule_pass_count INTEGER NOT NULL DEFAULT 0,
          llm_eval_count INTEGER NOT NULL DEFAULT 0,
          llm_input_tokens INTEGER NOT NULL DEFAULT 0,
          llm_output_tokens INTEGER NOT NULL DEFAULT 0,
          llm_spend_usd REAL NOT NULL DEFAULT 0.0,
          budget_over_target INTEGER NOT NULL DEFAULT 0,
          budget_note TEXT,
          error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS curation_candidates (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id INTEGER NOT NULL,
          post_id TEXT NOT NULL,
          source_id INTEGER,
          username TEXT,
          url TEXT NOT NULL,
          created_at TEXT,
          full_text TEXT,
          full_text_hash TEXT,
          representative_image_url TEXT,
          rule_score REAL NOT NULL DEFAULT 0,
          llm_total_score REAL NOT NULL DEFAULT 0,
          impact REAL NOT NULL DEFAULT 0,
          novelty REAL NOT NULL DEFAULT 0,
          utility REAL NOT NULL DEFAULT 0,
          accessibility REAL NOT NULL DEFAULT 0,
          difficulty INTEGER NOT NULL DEFAULT 1,
          flags_json TEXT,
          reasons_json TEXT,
          recommended_tier TEXT NOT NULL DEFAULT 'borderline',
          status TEXT NOT NULL DEFAULT 'pending',
          rank_order INTEGER NOT NULL DEFAULT 9999,
          generated_title TEXT,
          generated_summary TEXT,
          edited_title TEXT,
          edited_summary TEXT,
          edited_image_url TEXT,
          review_note TEXT,
          reviewed_by TEXT,
          reviewed_at TEXT,
          selected_for_publish INTEGER NOT NULL DEFAULT 1,
          FOREIGN KEY (run_id) REFERENCES curation_runs(id) ON DELETE CASCADE,
          UNIQUE(run_id, post_id)
        );

        CREATE TABLE IF NOT EXISTS curation_llm_cache (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          post_id TEXT NOT NULL,
          full_text_hash TEXT NOT NULL,
          model TEXT NOT NULL,
          prompt_version TEXT NOT NULL,
          task TEXT NOT NULL,
          result_json TEXT NOT NULL,
          input_tokens INTEGER NOT NULL DEFAULT 0,
          output_tokens INTEGER NOT NULL DEFAULT 0,
          cost_usd REAL NOT NULL DEFAULT 0.0,
          created_at TEXT NOT NULL,
          UNIQUE(post_id, full_text_hash, model, prompt_version, task)
        );

        CREATE TABLE IF NOT EXISTS curation_publications (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id INTEGER NOT NULL,
          feed_key TEXT NOT NULL DEFAULT 'ai-daily',
          published_at TEXT NOT NULL,
          publish_mode TEXT NOT NULL,
          published_by TEXT,
          item_count INTEGER NOT NULL DEFAULT 0,
          FOREIGN KEY (run_id) REFERENCES curation_runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS curation_publication_items (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          publication_id INTEGER NOT NULL,
          position INTEGER NOT NULL,
          post_id TEXT NOT NULL,
          username TEXT,
          url TEXT NOT NULL,
          created_at TEXT,
          title TEXT NOT NULL,
          summary TEXT NOT NULL,
          image_url TEXT,
          FOREIGN KEY (publication_id) REFERENCES curation_publications(id) ON DELETE CASCADE,
          UNIQUE(publication_id, position)
        );

        CREATE TABLE IF NOT EXISTS curated_rss_cache (
          feed_type TEXT NOT NULL,
          limit_count INTEGER NOT NULL,
          etag TEXT NOT NULL,
          last_modified TEXT,
          xml TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (feed_type, limit_count)
        );

        CREATE INDEX IF NOT EXISTS idx_curation_runs_date ON curation_runs(run_date_kst);
        CREATE INDEX IF NOT EXISTS idx_curation_candidates_run ON curation_candidates(run_id, rank_order);
        CREATE INDEX IF NOT EXISTS idx_curation_candidates_status ON curation_candidates(run_id, status);
        CREATE INDEX IF NOT EXISTS idx_curation_publications_run ON curation_publications(run_id);
        """
    )
    config_cols = {r[1] for r in conn.execute("PRAGMA table_info(curation_config)").fetchall()}
    if "openrouter_api_key" not in config_cols:
        conn.execute("ALTER TABLE curation_config ADD COLUMN openrouter_api_key TEXT")

    row = conn.execute("SELECT id FROM curation_config WHERE id = 1").fetchone()
    if not row:
        fields = list(DEFAULT_CURATION_CONFIG.keys())
        values = [DEFAULT_CURATION_CONFIG[k] for k in fields]
        values[fields.index("updated_at")] = now
        placeholders = ",".join(["?"] * len(fields))
        conn.execute(
            f"INSERT INTO curation_config ({','.join(fields)}) VALUES ({placeholders})",
            tuple(values),
        )
    conn.commit()


def get_config(conn) -> Dict[str, Any]:
    ensure_schema(conn)
    row = conn.execute("SELECT * FROM curation_config WHERE id = 1").fetchone()
    if not row:
        raise RuntimeError("curation_config missing")
    cols = [c[1] for c in conn.execute("PRAGMA table_info(curation_config)").fetchall()]
    data = dict(zip(cols, row))
    return data


def update_config(conn, updates: Dict[str, Any]) -> Dict[str, Any]:
    ensure_schema(conn)
    allowed = {
        "candidate_generation_enabled",
        "auto_publish_enabled",
        "rss_public_enabled",
        "target_daily_spend_usd",
        "daily_run_time_kst",
        "publish_delay_minutes",
        "feed_size",
        "window_hours",
        "candidate_pool_size",
        "w_impact",
        "w_novelty",
        "w_utility",
        "w_accessibility",
        "recommend_threshold",
        "min_utility",
        "min_accessibility",
        "pick_easy",
        "pick_deep",
        "pick_action",
        "openrouter_model",
        "openrouter_api_key",
        "input_cost_per_1m",
        "output_cost_per_1m",
        "prompt_version",
        "llm_enabled",
    }
    fields = []
    values: List[Any] = []
    for key, value in updates.items():
        if key in allowed and value is not None:
            if key == "openrouter_api_key" and isinstance(value, str):
                value = value.strip()
                if not value:
                    value = None
            fields.append(f"{key} = ?")
            values.append(value)
    if not fields:
        return get_config(conn)
    fields.append("updated_at = ?")
    values.append(datetime.now(UTC).isoformat())
    values.append(1)
    conn.execute(f"UPDATE curation_config SET {', '.join(fields)} WHERE id = ?", tuple(values))
    conn.commit()
    return get_config(conn)


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = date_parser.parse(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def _to_kst(dt: Optional[datetime]) -> Optional[datetime]:
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(KST)


def _kst_now() -> datetime:
    return datetime.now(UTC).astimezone(KST)


def _run_date_kst() -> str:
    return _kst_now().strftime("%Y-%m-%d")


def _avg(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _extract_roots_with_replies(
    conn,
    hours: int,
    scraped_since: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    cutoff = datetime.now(UTC) - timedelta(hours=hours)
    if scraped_since:
        if scraped_since.tzinfo is None:
            scraped_since = scraped_since.replace(tzinfo=UTC)
        else:
            scraped_since = scraped_since.astimezone(UTC)
        roots = conn.execute(
            "SELECT p.post_id, p.source_id, s.username, p.url, p.text, p.media_json, p.created_at, p.scraped_at "
            "FROM posts p JOIN feed_sources s ON s.id = p.source_id "
            "WHERE p.is_reply = 0 AND p.scraped_at >= ? ORDER BY p.created_at DESC LIMIT 2000",
            (scraped_since.isoformat(),),
        ).fetchall()
    else:
        roots = conn.execute(
            "SELECT p.post_id, p.source_id, s.username, p.url, p.text, p.media_json, p.created_at, p.scraped_at "
            "FROM posts p JOIN feed_sources s ON s.id = p.source_id "
            "WHERE p.is_reply = 0 ORDER BY p.created_at DESC LIMIT 2000"
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in roots:
        created = _parse_dt(r[6])
        if created and created < cutoff:
            continue
        replies = conn.execute(
            "SELECT text, media_json, created_at FROM posts WHERE source_id = ? AND is_reply = 1 AND parent_post_id = ? ORDER BY created_at ASC",
            (r[1], r[0]),
        ).fetchall()
        reply_texts = []
        media_urls = []
        root_media_urls = _parse_media_urls(r[5])
        if root_media_urls:
            media_urls.extend(root_media_urls)
        for rr in replies:
            rep_text = (rr[0] or "").strip()
            if rep_text:
                reply_texts.append(rep_text)
            rep_media = _parse_media_urls(rr[1])
            if rep_media:
                media_urls.extend(rep_media)
        seen = set()
        media_urls = [u for u in media_urls if u and not (u in seen or seen.add(u))]
        full_text = "\n".join([(r[4] or "").strip()] + reply_texts).strip()
        out.append(
            {
                "post_id": r[0],
                "source_id": r[1],
                "username": r[2],
                "url": r[3],
                "text": r[4] or "",
                "reply_texts": reply_texts,
                "full_text": full_text,
                "media_urls": media_urls,
                "representative_image_url": root_media_urls[0] if root_media_urls else (media_urls[0] if media_urls else None),
                "created_at": r[6],
                "scraped_at": r[7],
            }
        )
    return out


def _parse_media_urls(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if not isinstance(data, list):
            return []
        return [m.get("url") for m in data if isinstance(m, dict) and m.get("url")]
    except Exception:
        return []


def _rule_evaluate(post: Dict[str, Any]) -> Dict[str, Any]:
    text = (post.get("full_text") or "").strip()
    lower = text.lower()
    length = len(text)
    link_count = lower.count("http://") + lower.count("https://")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    sentence_count = max(1, len(lines))
    avg_line_len = length / sentence_count

    ai_keywords = ["ai", "gpt", "llm", "model", "agent", "prompt", "automation", "openai", "claude", "gemini", "rag"]
    utility_keywords = ["how", "방법", "워크플로우", "prompt", "template", "체크리스트", "설정", "code", "api", "자동화"]
    novelty_keywords = ["출시", "release", "update", "beta", "new", "런칭", "발표", "업데이트", "추가"]
    promo_keywords = ["구매", "할인", "판매", "문의", "링크클릭", "DM", "광고", "sponsor"]
    jargon_keywords = ["transformer", "backprop", "optimizer", "quantization", "vector db", "embedding", "tokenization"]

    ai_hit = any(k in lower for k in ai_keywords)
    utility_hit = sum(1 for k in utility_keywords if k in lower)
    novelty_hit = sum(1 for k in novelty_keywords if k in lower)
    promo_hit = any(k in lower for k in promo_keywords)
    jargon_hit = sum(1 for k in jargon_keywords if k in lower)

    flags: List[str] = []
    if length < 40:
        flags.append("too_short")
    if promo_hit:
        flags.append("ad_or_promo")
    if not ai_hit:
        flags.append("off_topic")
    if length < 120 and link_count == 0:
        flags.append("low_information_density")
    if avg_line_len > 180:
        flags.append("unclear_context")

    impact = _clip(35 + min(35, novelty_hit * 8) + min(20, link_count * 5) + min(10, len(post.get("reply_texts") or []) * 2))
    novelty = _clip(30 + min(45, novelty_hit * 12) + (10 if "today" in lower or "오늘" in lower else 0))
    utility = _clip(30 + min(45, utility_hit * 10) + min(10, link_count * 3) + (10 if length > 200 else 0))
    accessibility = _clip(85 - min(35, jargon_hit * 8) - (10 if avg_line_len > 140 else 0))
    difficulty = int(max(1, min(5, 1 + jargon_hit // 2 + (1 if avg_line_len > 160 else 0))))

    reasons = {
        "impact": "Rule-estimated impact",
        "novelty_or_newsworthiness": "Rule-estimated novelty",
        "utility": "Rule-estimated utility",
        "accessibility": "Rule-estimated accessibility",
        "difficulty": "Rule-estimated difficulty",
    }

    recommended = "borderline"
    if any(f in flags for f in ("too_short", "ad_or_promo", "off_topic")):
        recommended = "no"
    elif utility >= 65 and accessibility >= 55 and difficulty <= 3:
        recommended = "yes"

    return {
        "scores": {
            "impact": impact,
            "novelty_or_newsworthiness": novelty,
            "utility": utility,
            "accessibility": accessibility,
            "difficulty": difficulty,
        },
        "flags": flags,
        "reasons": reasons,
        "recommended_tier": recommended,
    }


def _clip(v: float) -> float:
    return float(max(0, min(100, round(v, 2))))


def _total_score(scores: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    return round(
        float(scores.get("impact", 0)) * float(cfg.get("w_impact", 0.35))
        + float(scores.get("novelty_or_newsworthiness", 0)) * float(cfg.get("w_novelty", 0.25))
        + float(scores.get("utility", 0)) * float(cfg.get("w_utility", 0.25))
        + float(scores.get("accessibility", 0)) * float(cfg.get("w_accessibility", 0.15)),
        3,
    )


def _call_openrouter_json(
    *,
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    timeout: int = 45,
) -> Tuple[Optional[Dict[str, Any]], int, int]:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    out = json.loads(raw)
    usage = out.get("usage") or {}
    in_tok = int(usage.get("prompt_tokens") or 0)
    out_tok = int(usage.get("completion_tokens") or 0)
    choices = out.get("choices") or []
    if not choices:
        return None, in_tok, out_tok
    content = choices[0].get("message", {}).get("content", "")
    if isinstance(content, list):
        content = "".join([str(x.get("text", "")) if isinstance(x, dict) else str(x) for x in content])
    content = str(content).strip()
    parsed = _extract_json(content)
    return parsed, in_tok, out_tok


def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    try:
        val = json.loads(text)
        if isinstance(val, dict):
            return val
    except Exception:
        pass
    s = text.find("{")
    e = text.rfind("}")
    if s >= 0 and e > s:
        try:
            val = json.loads(text[s : e + 1])
            if isinstance(val, dict):
                return val
        except Exception:
            return None
    return None


def _evaluate_with_llm(
    post: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    api_key: Optional[str],
) -> Tuple[Optional[Dict[str, Any]], int, int, float]:
    if not api_key or not int(cfg.get("llm_enabled", 1)):
        return None, 0, 0, 0.0
    system_prompt = (
        "You are an evaluator for AI-curation. Return strict JSON only.\n"
        "Schema: {scores:{impact,novelty_or_newsworthiness,utility,accessibility,difficulty},"
        "flags:[...],reasons:{...},recommended_tier:'yes|borderline|no'}\n"
        "scores are 0-100, difficulty is 1-5."
    )
    user_prompt = (
        f"post_id: {post.get('post_id')}\n"
        f"username: {post.get('username')}\n"
        f"created_at: {post.get('created_at')}\n"
        f"url: {post.get('url')}\n"
        f"text:\n{post.get('full_text') or ''}\n"
    )
    parsed, in_tok, out_tok = _call_openrouter_json(
        api_key=api_key,
        model=str(cfg.get("openrouter_model", "openai/gpt-4o-mini")),
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )
    cost = (
        (in_tok / 1_000_000.0) * float(cfg.get("input_cost_per_1m", 0))
        + (out_tok / 1_000_000.0) * float(cfg.get("output_cost_per_1m", 0))
    )
    return parsed, in_tok, out_tok, float(round(cost, 8))


def _resolve_openrouter_api_key(cfg: Dict[str, Any]) -> Optional[str]:
    key = str(cfg.get("openrouter_api_key") or "").strip()
    if key:
        return key
    return os.environ.get("OPENROUTER_API_KEY")


def _summarize_with_llm(
    post: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    api_key: Optional[str],
) -> Tuple[Optional[Dict[str, Any]], int, int, float]:
    if not api_key or not int(cfg.get("llm_enabled", 1)):
        return None, 0, 0, 0.0
    system_prompt = (
        "Create hook title and concise summary for curated RSS. Return strict JSON only.\n"
        "Schema: {title:string, summary:string}.\n"
        "Keep summary factual, short, no hype."
    )
    user_prompt = (
        f"post_id: {post.get('post_id')}\n"
        f"url: {post.get('url')}\n"
        f"text:\n{post.get('full_text') or ''}\n"
    )
    parsed, in_tok, out_tok = _call_openrouter_json(
        api_key=api_key,
        model=str(cfg.get("openrouter_model", "openai/gpt-4o-mini")),
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )
    cost = (
        (in_tok / 1_000_000.0) * float(cfg.get("input_cost_per_1m", 0))
        + (out_tok / 1_000_000.0) * float(cfg.get("output_cost_per_1m", 0))
    )
    return parsed, in_tok, out_tok, float(round(cost, 8))


def _upsert_llm_cache(
    conn,
    *,
    post_id: str,
    full_text_hash: str,
    model: str,
    prompt_version: str,
    task: str,
    result: Dict[str, Any],
    input_tokens: int,
    output_tokens: int,
    cost_usd: float,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO curation_llm_cache "
        "(post_id, full_text_hash, model, prompt_version, task, result_json, input_tokens, output_tokens, cost_usd, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            post_id,
            full_text_hash,
            model,
            prompt_version,
            task,
            json.dumps(result, ensure_ascii=False),
            input_tokens,
            output_tokens,
            cost_usd,
            datetime.now(UTC).isoformat(),
        ),
    )


def _get_llm_cache(
    conn,
    *,
    post_id: str,
    full_text_hash: str,
    model: str,
    prompt_version: str,
    task: str,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT result_json, input_tokens, output_tokens, cost_usd FROM curation_llm_cache "
        "WHERE post_id = ? AND full_text_hash = ? AND model = ? AND prompt_version = ? AND task = ?",
        (post_id, full_text_hash, model, prompt_version, task),
    ).fetchone()
    if not row:
        return None
    try:
        parsed = json.loads(row[0]) if row[0] else {}
    except Exception:
        parsed = {}
    return {
        "result": parsed if isinstance(parsed, dict) else {},
        "input_tokens": int(row[1] or 0),
        "output_tokens": int(row[2] or 0),
        "cost_usd": float(row[3] or 0.0),
    }


def _select_ranked_candidates(candidates: List[Dict[str, Any]], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Keep distribution loosely aligned with n8n config.
    feed_size = int(cfg.get("feed_size", 10))
    pick_easy = int(cfg.get("pick_easy", 6))
    pick_deep = int(cfg.get("pick_deep", 2))
    pick_action = int(cfg.get("pick_action", 2))

    sorted_items = sorted(candidates, key=lambda x: (x["rank_score"], x["utility"]), reverse=True)
    easy = [c for c in sorted_items if int(c.get("difficulty", 1)) <= 2]
    deep = [c for c in sorted_items if int(c.get("difficulty", 1)) >= 3]
    action = [c for c in sorted_items if float(c.get("utility", 0)) >= 75]

    out: List[Dict[str, Any]] = []
    seen = set()

    def _take(pool: List[Dict[str, Any]], n: int) -> None:
        for c in pool:
            if len(out) >= feed_size or n <= 0:
                return
            if c["post_id"] in seen:
                continue
            seen.add(c["post_id"])
            out.append(c)
            n -= 1

    _take(easy, pick_easy)
    _take(deep, pick_deep)
    _take(action, pick_action)
    _take(sorted_items, feed_size - len(out))
    return out[:feed_size]


def run_curation_once(
    conn,
    *,
    force: bool = False,
    scraped_since: Optional[datetime] = None,
    logger: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    ensure_schema(conn)
    cfg = get_config(conn)
    if not force and not bool(cfg.get("candidate_generation_enabled", 1)):
        return {"status": "skipped", "reason": "candidate_generation_disabled"}

    run_date = _run_date_kst()
    row = conn.execute(
        "SELECT id, status FROM curation_runs WHERE run_date_kst = ? ORDER BY id DESC LIMIT 1",
        (run_date,),
    ).fetchone()
    if row and not force and row[1] in ("ready", "published", "running"):
        return {"status": "skipped", "reason": "already_generated", "run_id": row[0]}

    started_at = datetime.now(UTC).isoformat()
    if row and force:
        run_id = int(row[0])
        conn.execute("DELETE FROM curation_publications WHERE run_id = ?", (run_id,))
        conn.execute("DELETE FROM curation_candidates WHERE run_id = ?", (run_id,))
        conn.execute(
            "UPDATE curation_runs SET status='running', started_at=?, finished_at=NULL, "
            "total_posts=0, rule_pass_count=0, llm_eval_count=0, llm_input_tokens=0, "
            "llm_output_tokens=0, llm_spend_usd=0.0, budget_over_target=0, budget_note=NULL, error_message=NULL "
            "WHERE id = ?",
            (started_at, run_id),
        )
    else:
        conn.execute(
            "INSERT INTO curation_runs (run_date_kst, status, started_at) VALUES (?, 'running', ?)",
            (run_date, started_at),
        )
        run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()

    log = logger or (lambda _: None)
    try:
        posts = _extract_roots_with_replies(
            conn,
            int(cfg.get("window_hours", 24)),
            scraped_since=scraped_since,
        )
        rule_evaluated = []
        for post in posts:
            rule = _rule_evaluate(post)
            scores = rule.get("scores", {})
            rank_score = _total_score(scores, cfg)
            post_data = {
                **post,
                "rule": rule,
                "rank_score": rank_score,
                "impact": float(scores.get("impact", 0)),
                "novelty": float(scores.get("novelty_or_newsworthiness", 0)),
                "utility": float(scores.get("utility", 0)),
                "accessibility": float(scores.get("accessibility", 0)),
                "difficulty": int(scores.get("difficulty", 1)),
            }
            rule_evaluated.append(post_data)

        # keep only likely relevant posts first
        filtered = [
            x
            for x in rule_evaluated
            if "off_topic" not in (x["rule"].get("flags") or []) and "too_short" not in (x["rule"].get("flags") or [])
        ]
        if not filtered:
            filtered = rule_evaluated

        filtered.sort(key=lambda x: x["rank_score"], reverse=True)
        candidate_pool = filtered[: int(cfg.get("candidate_pool_size", 20))]

        api_key = _resolve_openrouter_api_key(cfg)
        model = str(cfg.get("openrouter_model", "openai/gpt-4o-mini"))
        prompt_version = str(cfg.get("prompt_version", "v1"))

        total_in_tokens = 0
        total_out_tokens = 0
        total_cost = 0.0
        llm_count = 0

        enriched: List[Dict[str, Any]] = []
        for post in candidate_pool:
            full_hash = hashlib.sha256((post.get("full_text") or "").encode("utf-8")).hexdigest()
            cache = _get_llm_cache(
                conn,
                post_id=post["post_id"],
                full_text_hash=full_hash,
                model=model,
                prompt_version=prompt_version,
                task="eval",
            )
            llm_eval = None
            if cache:
                llm_eval = cache["result"]
                total_in_tokens += cache["input_tokens"]
                total_out_tokens += cache["output_tokens"]
                total_cost += cache["cost_usd"]
            else:
                parsed, in_tok, out_tok, cost = _evaluate_with_llm(post, cfg, api_key=api_key)
                if parsed:
                    llm_eval = parsed
                    _upsert_llm_cache(
                        conn,
                        post_id=post["post_id"],
                        full_text_hash=full_hash,
                        model=model,
                        prompt_version=prompt_version,
                        task="eval",
                        result=parsed,
                        input_tokens=in_tok,
                        output_tokens=out_tok,
                        cost_usd=cost,
                    )
                    total_in_tokens += in_tok
                    total_out_tokens += out_tok
                    total_cost += cost
                    llm_count += 1

            merged = post.copy()
            final_rule = post["rule"]
            if isinstance(llm_eval, dict) and llm_eval.get("scores"):
                final_rule = llm_eval
            merged["final_eval"] = final_rule
            fs = final_rule.get("scores", {})
            merged["impact"] = float(fs.get("impact", merged["impact"]))
            merged["novelty"] = float(fs.get("novelty_or_newsworthiness", merged["novelty"]))
            merged["utility"] = float(fs.get("utility", merged["utility"]))
            merged["accessibility"] = float(fs.get("accessibility", merged["accessibility"]))
            merged["difficulty"] = int(fs.get("difficulty", merged["difficulty"]))
            merged["rank_score"] = _total_score(fs, cfg)
            merged["recommended_tier"] = str(final_rule.get("recommended_tier", "borderline"))
            enriched.append(merged)

        # keep yes/borderline and threshold conditions
        min_utility = float(cfg.get("min_utility", 60))
        min_accessibility = float(cfg.get("min_accessibility", 55))
        threshold = float(cfg.get("recommend_threshold", 75))
        picked = []
        for item in sorted(enriched, key=lambda x: x["rank_score"], reverse=True):
            tier = item.get("recommended_tier", "borderline")
            if tier == "no":
                continue
            if item["utility"] < min_utility and item["accessibility"] < min_accessibility:
                continue
            if tier == "yes" or item["rank_score"] >= threshold or item["utility"] >= min_utility:
                picked.append(item)
        if not picked:
            picked = sorted(enriched, key=lambda x: x["rank_score"], reverse=True)

        ranked = _select_ranked_candidates(picked, cfg)

        conn.execute("DELETE FROM curation_candidates WHERE run_id = ?", (run_id,))
        for idx, item in enumerate(ranked, start=1):
            eval_data = item.get("final_eval") or item.get("rule") or {}
            conn.execute(
                "INSERT INTO curation_candidates "
                "(run_id, post_id, source_id, username, url, created_at, full_text, full_text_hash, representative_image_url, "
                "rule_score, llm_total_score, impact, novelty, utility, accessibility, difficulty, flags_json, reasons_json, recommended_tier, status, rank_order, selected_for_publish) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, 1)",
                (
                    run_id,
                    item["post_id"],
                    item.get("source_id"),
                    item.get("username"),
                    item.get("url"),
                    item.get("created_at"),
                    item.get("full_text"),
                    hashlib.sha256((item.get("full_text") or "").encode("utf-8")).hexdigest(),
                    item.get("representative_image_url"),
                    float(item.get("rank_score", 0)),
                    float(item.get("rank_score", 0)),
                    float(item.get("impact", 0)),
                    float(item.get("novelty", 0)),
                    float(item.get("utility", 0)),
                    float(item.get("accessibility", 0)),
                    int(item.get("difficulty", 1)),
                    json.dumps(eval_data.get("flags", []), ensure_ascii=False),
                    json.dumps(eval_data.get("reasons", {}), ensure_ascii=False),
                    str(item.get("recommended_tier", "borderline")),
                    idx,
                ),
            )

        # soft target only: never stop, only flag
        target = float(cfg.get("target_daily_spend_usd", 1.0))
        day_spend = conn.execute(
            "SELECT COALESCE(SUM(llm_spend_usd), 0) FROM curation_runs WHERE run_date_kst = ?",
            (run_date,),
        ).fetchone()[0]
        over_target = 1 if float(day_spend) + float(total_cost) > target else 0
        budget_note = None
        if over_target:
            budget_note = f"soft target exceeded: target={target:.4f}, actual={(float(day_spend)+float(total_cost)):.4f}"
            log(f"[curation] {budget_note}")

        finished = datetime.now(UTC).isoformat()
        conn.execute(
            "UPDATE curation_runs SET status='ready', finished_at=?, total_posts=?, rule_pass_count=?, llm_eval_count=?, "
            "llm_input_tokens=?, llm_output_tokens=?, llm_spend_usd=?, budget_over_target=?, budget_note=? WHERE id=?",
            (
                finished,
                len(posts),
                len(candidate_pool),
                llm_count,
                total_in_tokens,
                total_out_tokens,
                float(round(total_cost, 8)),
                over_target,
                budget_note,
                run_id,
            ),
        )
        conn.commit()
        log(
            "[curation] run ready "
            f"run_id={run_id} total_posts={len(posts)} pool={len(candidate_pool)} llm_eval={llm_count} "
            f"spend_usd={float(round(total_cost, 6))} over_target={bool(over_target)}"
        )
        return {"status": "ok", "run_id": run_id}
    except Exception as e:
        conn.execute(
            "UPDATE curation_runs SET status='failed', finished_at=?, error_message=? WHERE id=?",
            (datetime.now(UTC).isoformat(), str(e), run_id),
        )
        conn.commit()
        raise


def _fallback_title(text: str) -> str:
    one = (text or "").strip().split("\n")[0]
    if not one:
        return "AI 업데이트 요약"
    return one[:40]


def _fallback_summary(text: str) -> str:
    clean = " ".join((text or "").strip().split())
    if not clean:
        return "원문 요약 정보가 없습니다."
    return clean[:240]


def _resolve_summary_for_candidate(conn, cand: Dict[str, Any], cfg: Dict[str, Any]) -> Tuple[str, str, int, int, float]:
    # priority: edited -> generated -> llm(summary) -> fallback
    edited_title = (cand.get("edited_title") or "").strip()
    edited_summary = (cand.get("edited_summary") or "").strip()
    if edited_title and edited_summary:
        return edited_title, edited_summary, 0, 0, 0.0

    generated_title = (cand.get("generated_title") or "").strip()
    generated_summary = (cand.get("generated_summary") or "").strip()
    if generated_title and generated_summary:
        return generated_title, generated_summary, 0, 0, 0.0

    post = {
        "post_id": cand.get("post_id"),
        "url": cand.get("url"),
        "full_text": cand.get("full_text") or "",
    }
    full_hash = hashlib.sha256((post["full_text"]).encode("utf-8")).hexdigest()
    model = str(cfg.get("openrouter_model", "openai/gpt-4o-mini"))
    prompt_version = str(cfg.get("prompt_version", "v1"))
    cache = _get_llm_cache(
        conn,
        post_id=post["post_id"],
        full_text_hash=full_hash,
        model=model,
        prompt_version=prompt_version,
        task="summary",
    )
    if cache:
        data = cache["result"]
        title = str(data.get("title") or "").strip() or _fallback_title(post["full_text"])
        summary = str(data.get("summary") or "").strip() or _fallback_summary(post["full_text"])
        return title, summary, cache["input_tokens"], cache["output_tokens"], cache["cost_usd"]

    api_key = _resolve_openrouter_api_key(cfg)
    parsed, in_tok, out_tok, cost = _summarize_with_llm(post, cfg, api_key=api_key)
    if parsed:
        _upsert_llm_cache(
            conn,
            post_id=post["post_id"],
            full_text_hash=full_hash,
            model=model,
            prompt_version=prompt_version,
            task="summary",
            result=parsed,
            input_tokens=in_tok,
            output_tokens=out_tok,
            cost_usd=cost,
        )
        title = str(parsed.get("title") or "").strip() or _fallback_title(post["full_text"])
        summary = str(parsed.get("summary") or "").strip() or _fallback_summary(post["full_text"])
        return title, summary, in_tok, out_tok, cost

    return _fallback_title(post["full_text"]), _fallback_summary(post["full_text"]), 0, 0, 0.0


def _latest_ready_run(conn) -> Optional[int]:
    row = conn.execute(
        "SELECT id FROM curation_runs WHERE status IN ('ready','published') ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return int(row[0]) if row else None


def latest_ready_run(conn) -> Optional[int]:
    return _latest_ready_run(conn)


def list_runs(conn, limit: int = 30) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT id, run_date_kst, status, started_at, finished_at, total_posts, rule_pass_count, llm_eval_count, "
        "llm_input_tokens, llm_output_tokens, llm_spend_usd, budget_over_target, budget_note, error_message "
        "FROM curation_runs ORDER BY id DESC LIMIT ?",
        (max(1, min(200, limit)),),
    ).fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "run_date_kst": r[1],
                "status": r[2],
                "started_at": r[3],
                "finished_at": r[4],
                "total_posts": r[5],
                "rule_pass_count": r[6],
                "llm_eval_count": r[7],
                "llm_input_tokens": r[8],
                "llm_output_tokens": r[9],
                "llm_spend_usd": r[10],
                "budget_over_target": bool(r[11]),
                "budget_note": r[12],
                "error_message": r[13],
            }
        )
    return out


def list_candidates(conn, run_id: Optional[int]) -> Dict[str, Any]:
    if run_id is None:
        run_id = _latest_ready_run(conn)
    if not run_id:
        return {"run_id": None, "candidates": []}
    rows = conn.execute(
        "SELECT id, post_id, username, url, created_at, representative_image_url, rule_score, llm_total_score, "
        "impact, novelty, utility, accessibility, difficulty, flags_json, reasons_json, recommended_tier, status, "
        "rank_order, generated_title, generated_summary, edited_title, edited_summary, edited_image_url, selected_for_publish "
        "FROM curation_candidates WHERE run_id = ? ORDER BY rank_order ASC, id ASC",
        (run_id,),
    ).fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "post_id": r[1],
                "username": r[2],
                "url": r[3],
                "created_at": r[4],
                "representative_image_url": r[5],
                "rule_score": r[6],
                "llm_total_score": r[7],
                "impact": r[8],
                "novelty": r[9],
                "utility": r[10],
                "accessibility": r[11],
                "difficulty": r[12],
                "flags": json.loads(r[13] or "[]"),
                "reasons": json.loads(r[14] or "{}"),
                "recommended_tier": r[15],
                "status": r[16],
                "rank_order": r[17],
                "generated_title": r[18],
                "generated_summary": r[19],
                "edited_title": r[20],
                "edited_summary": r[21],
                "edited_image_url": r[22],
                "selected_for_publish": bool(r[23]),
            }
        )
    return {"run_id": run_id, "candidates": out}


def update_candidate(conn, candidate_id: int, payload: Dict[str, Any], reviewed_by: str = "admin") -> None:
    fields = []
    values: List[Any] = []
    if "action" in payload:
        action = (payload.get("action") or "").strip().lower()
        if action == "approve":
            fields.append("status = ?")
            values.append("approved")
        elif action == "reject":
            fields.append("status = ?")
            values.append("rejected")
        elif action == "pending":
            fields.append("status = ?")
            values.append("pending")
    if "edited_title" in payload:
        fields.append("edited_title = ?")
        values.append(payload.get("edited_title"))
    if "edited_summary" in payload:
        fields.append("edited_summary = ?")
        values.append(payload.get("edited_summary"))
    if "edited_image_url" in payload:
        fields.append("edited_image_url = ?")
        values.append(payload.get("edited_image_url"))
    if "selected_for_publish" in payload:
        fields.append("selected_for_publish = ?")
        values.append(1 if payload.get("selected_for_publish") else 0)
    if "review_note" in payload:
        fields.append("review_note = ?")
        values.append(payload.get("review_note"))
    if not fields:
        return
    fields.append("reviewed_by = ?")
    values.append(reviewed_by)
    fields.append("reviewed_at = ?")
    values.append(datetime.now(UTC).isoformat())
    values.append(candidate_id)
    conn.execute(f"UPDATE curation_candidates SET {', '.join(fields)} WHERE id = ?", tuple(values))
    conn.commit()


def approve_all(conn, run_id: int, reviewed_by: str = "admin") -> None:
    now = datetime.now(UTC).isoformat()
    conn.execute(
        "UPDATE curation_candidates SET status='approved', reviewed_by=?, reviewed_at=? "
        "WHERE run_id=? AND status!='rejected'",
        (reviewed_by, now, run_id),
    )
    conn.commit()


def reorder_candidates(conn, run_id: int, ordered_ids: List[int]) -> None:
    pos = 1
    for cid in ordered_ids:
        conn.execute(
            "UPDATE curation_candidates SET rank_order = ? WHERE run_id = ? AND id = ?",
            (pos, run_id, cid),
        )
        pos += 1
    conn.commit()


def publish_run(
    conn,
    run_id: int,
    *,
    publish_mode: str = "manual",
    published_by: str = "admin",
    logger: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    ensure_schema(conn)
    cfg = get_config(conn)
    feed_size = int(cfg.get("feed_size", 10))
    rows = conn.execute(
        "SELECT id, post_id, username, url, created_at, full_text, representative_image_url, "
        "edited_title, edited_summary, edited_image_url, generated_title, generated_summary, status, rank_order, selected_for_publish "
        "FROM curation_candidates WHERE run_id = ? ORDER BY rank_order ASC, id ASC",
        (run_id,),
    ).fetchall()
    if not rows:
        raise ValueError("no candidates in run")

    approved = [r for r in rows if r[12] == "approved" and int(r[14] or 1) == 1]
    pending = [r for r in rows if r[12] in ("pending", "approved") and int(r[14] or 1) == 1]
    selected_rows = approved[:feed_size]
    if len(selected_rows) < feed_size:
        # fill from non-rejected candidates
        selected_ids = {r[0] for r in selected_rows}
        for r in pending:
            if r[0] in selected_ids:
                continue
            selected_rows.append(r)
            if len(selected_rows) >= feed_size:
                break

    if not selected_rows:
        raise ValueError("no publishable candidates")

    total_in = 0
    total_out = 0
    total_cost = 0.0

    pub_at = datetime.now(UTC).isoformat()
    conn.execute(
        "INSERT INTO curation_publications (run_id, feed_key, published_at, publish_mode, published_by, item_count) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, FEED_KEY, pub_at, publish_mode, published_by, len(selected_rows)),
    )
    publication_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    for pos, r in enumerate(selected_rows, start=1):
        cand = {
            "id": r[0],
            "post_id": r[1],
            "username": r[2],
            "url": r[3],
            "created_at": r[4],
            "full_text": r[5] or "",
            "representative_image_url": r[6],
            "edited_title": r[7],
            "edited_summary": r[8],
            "edited_image_url": r[9],
            "generated_title": r[10],
            "generated_summary": r[11],
        }
        title, summary, in_tok, out_tok, cost = _resolve_summary_for_candidate(conn, cand, cfg)
        total_in += in_tok
        total_out += out_tok
        total_cost += cost

        # Persist generated fields for future edits if they were empty.
        if not cand["generated_title"] and title:
            conn.execute("UPDATE curation_candidates SET generated_title=? WHERE id=?", (title, cand["id"]))
        if not cand["generated_summary"] and summary:
            conn.execute("UPDATE curation_candidates SET generated_summary=? WHERE id=?", (summary, cand["id"]))

        image_url = (cand["edited_image_url"] or "").strip() or (cand["representative_image_url"] or "")
        kst = _to_kst(_parse_dt(cand["created_at"]))
        date_prefix = kst.strftime("%m.%d") if kst else _kst_now().strftime("%m.%d")
        final_title = (cand["edited_title"] or "").strip() or title
        if not final_title:
            final_title = _fallback_title(cand["full_text"])
        display_title = f'{date_prefix} - "{final_title}"'
        final_summary = (cand["edited_summary"] or "").strip() or summary or _fallback_summary(cand["full_text"])

        conn.execute(
            "INSERT INTO curation_publication_items "
            "(publication_id, position, post_id, username, url, created_at, title, summary, image_url) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                publication_id,
                pos,
                cand["post_id"],
                cand["username"],
                cand["url"],
                cand["created_at"],
                display_title,
                final_summary,
                image_url or None,
            ),
        )

    conn.execute("UPDATE curation_runs SET status='published' WHERE id = ?", (run_id,))
    # remove stale cache on each publication
    conn.execute("DELETE FROM curated_rss_cache")
    # include summary-generation cost into run accounting as well
    conn.execute(
        "UPDATE curation_runs SET llm_input_tokens = llm_input_tokens + ?, llm_output_tokens = llm_output_tokens + ?, llm_spend_usd = llm_spend_usd + ? WHERE id = ?",
        (total_in, total_out, float(round(total_cost, 8)), run_id),
    )

    # soft-target recalc after publish-time summary costs
    cfg = get_config(conn)
    target = float(cfg.get("target_daily_spend_usd", 1.0))
    run_row = conn.execute("SELECT run_date_kst, llm_spend_usd FROM curation_runs WHERE id=?", (run_id,)).fetchone()
    if run_row:
        run_date = run_row[0]
        day_spend = conn.execute(
            "SELECT COALESCE(SUM(llm_spend_usd),0) FROM curation_runs WHERE run_date_kst = ?",
            (run_date,),
        ).fetchone()[0]
        over = 1 if float(day_spend) > target else 0
        note = None
        if over:
            note = f"soft target exceeded: target={target:.4f}, actual={float(day_spend):.4f}"
        conn.execute(
            "UPDATE curation_runs SET budget_over_target=?, budget_note=? WHERE id=?",
            (over, note, run_id),
        )

    conn.commit()
    if logger:
        logger(
            f"[curation] published run_id={run_id} mode={publish_mode} items={len(selected_rows)} "
            f"summary_cost_usd={float(round(total_cost, 6))}"
        )
    return {"publication_id": publication_id, "item_count": len(selected_rows)}


def maybe_auto_publish_due(conn, logger: Optional[Callable[[str], None]] = None) -> Optional[Dict[str, Any]]:
    cfg = get_config(conn)
    if not bool(cfg.get("auto_publish_enabled", 1)):
        return None
    delay = int(cfg.get("publish_delay_minutes", 60))
    now = datetime.now(UTC)
    rows = conn.execute(
        "SELECT id, finished_at FROM curation_runs WHERE status='ready' ORDER BY id DESC LIMIT 5"
    ).fetchall()
    for r in rows:
        run_id = int(r[0])
        finished = _parse_dt(r[1])
        if not finished:
            continue
        if now < finished + timedelta(minutes=delay):
            continue
        has_pub = conn.execute(
            "SELECT id FROM curation_publications WHERE run_id=? LIMIT 1",
            (run_id,),
        ).fetchone()
        if has_pub:
            continue
        return publish_run(conn, run_id, publish_mode="auto", published_by="system:auto", logger=logger)
    return None


def maybe_generate_daily(conn, logger: Optional[Callable[[str], None]] = None) -> Optional[Dict[str, Any]]:
    cfg = get_config(conn)
    if not bool(cfg.get("candidate_generation_enabled", 1)):
        return None
    run_time = str(cfg.get("daily_run_time_kst", "06:00"))
    try:
        hh, mm = run_time.split(":")
        hh_i, mm_i = int(hh), int(mm)
    except Exception:
        hh_i, mm_i = 6, 0
    now_kst = _kst_now()
    if (now_kst.hour, now_kst.minute) < (hh_i, mm_i):
        return None
    return run_curation_once(conn, force=False, logger=logger)


def _latest_publication(conn) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT id, run_id, published_at, publish_mode, published_by, item_count FROM curation_publications WHERE feed_key=? ORDER BY id DESC LIMIT 1",
        (FEED_KEY,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "run_id": row[1],
        "published_at": row[2],
        "publish_mode": row[3],
        "published_by": row[4],
        "item_count": row[5],
    }


def latest_publication(conn) -> Optional[Dict[str, Any]]:
    return _latest_publication(conn)


def _publication_items(conn, publication_id: int, limit: int) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT position, post_id, username, url, created_at, title, summary, image_url "
        "FROM curation_publication_items WHERE publication_id=? ORDER BY position ASC LIMIT ?",
        (publication_id, limit),
    ).fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "position": r[0],
                "post_id": r[1],
                "username": r[2],
                "url": r[3],
                "created_at": r[4],
                "title": r[5],
                "summary": r[6],
                "image_url": r[7],
            }
        )
    return out


def _digest_body(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "<p>No curated items.</p>"
    first = items[0]
    top_line = f'"{xml_escape(first.get("title") or "")}"'
    parts = [f"<p>{top_line}</p>"]
    for index, it in enumerate(items):
        kst = _to_kst(_parse_dt(it.get("created_at")))
        dt_text = kst.strftime("%m.%d %H:%M (KST)") if kst else ""
        title = xml_escape(it.get("title") or "")
        summary = xml_escape(it.get("summary") or "")
        url = xml_escape(it.get("url") or "")
        image_url = (it.get("image_url") or "").strip()
        if index > 0:
            parts.append("<hr style=\"border:0;border-top:1px solid #eee7db;margin:10px 0;opacity:0.7;\" />")
        item_parts = [f"<p>{it.get('position')}. \"{title}\"</p>"]
        if image_url:
            item_parts.append(f"<img src=\"{xml_escape(image_url)}\" />")
        item_parts.append(f"<p>{summary}</p>")
        item_parts.append(f"<p>원문: <a href=\"{url}\">{url}</a></p>")
        if dt_text:
            item_parts.append(f"<p>작성: {xml_escape(dt_text)}</p>")
        parts.append("".join(item_parts))
    return "".join(parts)


def _build_item_feed_xml(publication: Dict[str, Any], items: List[Dict[str, Any]], limit: int) -> Tuple[str, str, Optional[str]]:
    ch_title = "AI Curated Threads Feed"
    ch_link = "https://thread-collector.jotto.in/admin/curation"
    ch_desc = "Manually reviewed AI curation feed"
    rss_items = []
    last_modified = None
    if items:
        dt = _parse_dt(items[0].get("created_at"))
        if dt:
            last_modified = format_datetime(dt)
    for it in items[:limit]:
        dt = _parse_dt(it.get("created_at")) or datetime.now(UTC)
        pub = format_datetime(dt)
        image_url = (it.get("image_url") or "").strip()
        summary = it.get("summary") or ""
        youtube_embeds = collect_youtube_embeds(summary, [])
        content_blocks = [{"text": summary, "media_urls": [image_url] if image_url else []}]
        content_html = build_content_html(
            root_text=summary,
            reply_texts=[],
            media_urls=[image_url] if image_url else [],
            youtube_embeds=youtube_embeds,
            content_blocks=content_blocks,
        )
        desc = build_description_html(summary, [])
        enclosures = build_enclosures([image_url] if image_url else [])
        media = build_media_contents([image_url] if image_url else [])
        players = build_media_players(youtube_embeds)
        rss_items.append(
            f"<item>"
            f"<title>{xml_escape(it.get('title') or '')}</title>"
            f"<link>{xml_escape(it.get('url') or '')}</link>"
            f"<guid>{xml_escape(f'curated-{publication['id']}-{it.get('position')}-{it.get('post_id')}')}</guid>"
            f"<pubDate>{pub}</pubDate>"
            f"<description>{desc}</description>"
            f"<content:encoded><![CDATA[{content_html}]]></content:encoded>"
            f"{enclosures}{media}{players}"
            f"</item>"
        )
    xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<?xml-stylesheet type=\"text/xsl\" href=\"/rss.xsl?v=2\"?>"
        "<rss version=\"2.0\" xmlns:content=\"http://purl.org/rss/1.0/modules/content/\" xmlns:media=\"http://search.yahoo.com/mrss/\">"
        "<channel>"
        f"<title>{xml_escape(ch_title)}</title>"
        f"<link>{xml_escape(ch_link)}</link>"
        f"<description>{xml_escape(ch_desc)}</description>"
        + "".join(rss_items)
        + "</channel></rss>"
    )
    return xml, hashlib.sha256(xml.encode("utf-8")).hexdigest(), last_modified


def _build_digest_feed_xml(publication: Dict[str, Any], items: List[Dict[str, Any]]) -> Tuple[str, str, Optional[str]]:
    dt = _to_kst(_parse_dt(publication.get("published_at"))) or _kst_now()
    ch_title = "AI Curated Daily Digest"
    ch_link = "https://thread-collector.jotto.in/admin/curation"
    ch_desc = "Daily digest curated from top AI threads"
    digest_title = f"{dt.strftime('%m.%d')} AI 큐레이션 TOP 10"
    digest_link = ch_link
    digest_body = _digest_body(items)
    digest_desc = "Daily curated digest"
    pub_date = format_datetime(_parse_dt(publication.get("published_at")) or datetime.now(UTC))
    last_modified = pub_date
    xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<?xml-stylesheet type=\"text/xsl\" href=\"/rss.xsl?v=2\"?>"
        "<rss version=\"2.0\" xmlns:content=\"http://purl.org/rss/1.0/modules/content/\" xmlns:media=\"http://search.yahoo.com/mrss/\">"
        "<channel>"
        f"<title>{xml_escape(ch_title)}</title>"
        f"<link>{xml_escape(ch_link)}</link>"
        f"<description>{xml_escape(ch_desc)}</description>"
        "<item>"
        f"<title>{xml_escape(digest_title)}</title>"
        f"<link>{xml_escape(digest_link)}</link>"
        f"<guid>{xml_escape(f'digest-{publication['id']}')}</guid>"
        f"<pubDate>{pub_date}</pubDate>"
        f"<description>{xml_escape(digest_desc)}</description>"
        f"<content:encoded><![CDATA[{digest_body}]]></content:encoded>"
        "</item>"
        "</channel></rss>"
    )
    return xml, hashlib.sha256(xml.encode("utf-8")).hexdigest(), last_modified


def build_feed_xml(conn, feed_type: str, limit: int = 10) -> Tuple[str, str, Optional[str]]:
    publication = _latest_publication(conn)
    if not publication:
        # empty feed to keep readers stable
        empty = (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<rss version=\"2.0\"><channel><title>AI Curated Feed</title><link>https://thread-collector.jotto.in</link>"
            "<description>No publication yet</description></channel></rss>"
        )
        return empty, hashlib.sha256(empty.encode("utf-8")).hexdigest(), None
    items = _publication_items(conn, publication["id"], max(1, min(100, limit)))
    if feed_type == DIGEST_FEED:
        return _build_digest_feed_xml(publication, items[:10])
    return _build_item_feed_xml(publication, items, limit=limit)


def refresh_cache(conn, feed_type: str, limit: int = 10) -> Dict[str, Any]:
    xml, etag, last_modified = build_feed_xml(conn, feed_type, limit)
    now = datetime.now(UTC).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO curated_rss_cache (feed_type, limit_count, etag, last_modified, xml, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (feed_type, limit, etag, last_modified, xml, now),
    )
    conn.commit()
    return {"xml": xml, "etag": etag, "last_modified": last_modified}


def get_cached_feed(conn, feed_type: str, limit: int = 10) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT etag, last_modified, xml, updated_at FROM curated_rss_cache WHERE feed_type = ? AND limit_count = ?",
        (feed_type, limit),
    ).fetchone()
    if row:
        return {"etag": row[0], "last_modified": row[1], "xml": row[2], "updated_at": row[3]}
    return refresh_cache(conn, feed_type, limit)


def stats(conn, days: int = 30) -> Dict[str, Any]:
    days = max(1, min(365, days))
    since = (_kst_now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT run_date_kst, llm_spend_usd, budget_over_target, total_posts, rule_pass_count "
        "FROM curation_runs WHERE run_date_kst >= ? ORDER BY run_date_kst ASC",
        (since,),
    ).fetchall()
    spends = [float(r[1] or 0.0) for r in rows]
    posts = [int(r[3] or 0) for r in rows]
    pubs = conn.execute(
        "SELECT COUNT(*), COALESCE(AVG(item_count),0) FROM curation_publications WHERE published_at >= ?",
        ((_kst_now() - timedelta(days=days)).astimezone(UTC).isoformat(),),
    ).fetchone()
    return {
        "days": days,
        "daily": [
            {
                "run_date_kst": r[0],
                "llm_spend_usd": float(r[1] or 0.0),
                "budget_over_target": bool(r[2]),
                "total_posts": int(r[3] or 0),
                "rule_pass_count": int(r[4] or 0),
            }
            for r in rows
        ],
        "avg_daily_spend_7d": _avg(spends[-7:]),
        "avg_daily_spend_30d": _avg(spends[-30:]),
        "avg_posts_per_run": _avg([float(x) for x in posts]) if posts else 0.0,
        "total_publications": int(pubs[0] or 0),
        "avg_items_per_publication": float(pubs[1] or 0.0),
    }

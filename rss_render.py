"""Shared RSS rendering helpers for API and cache builder."""

import html
import re
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import parse_qs, unquote, urlparse

_URL_RE = re.compile(r"https?://[^\s<>'\"]+")
_YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
_BLOCK_DIVIDER_HTML = (
    "<hr style=\"border:0;border-top:1px solid #eee7db;margin:10px 0;opacity:0.7;\" />"
)


def xml_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def guess_mime(url: str) -> str:
    raw = _normalize_media_url(url)
    nested = _unwrap_nested_url(raw)
    lower_candidates = [raw.lower()]
    if nested:
        lower_candidates.append(nested.lower())

    if _contains_any(lower_candidates, [".jpg", ".jpeg"]):
        return "image/jpeg"
    if _contains_any(lower_candidates, [".png"]):
        return "image/png"
    if _contains_any(lower_candidates, [".webp"]):
        return "image/webp"
    if _contains_any(lower_candidates, [".gif"]):
        return "image/gif"
    if _contains_any(lower_candidates, [".mp4"]):
        return "video/mp4"
    if _contains_any(lower_candidates, [".webm"]):
        return "video/webm"
    return "application/octet-stream"


def reply_texts_from_tuples(replies: Sequence[tuple]) -> List[str]:
    texts: List[str] = []
    for reply in replies or []:
        if not reply:
            continue
        text = (reply[0] or "").strip()
        if text:
            texts.append(text)
    return texts


def reply_texts_from_dicts(replies: Sequence[Dict[str, Any]]) -> List[str]:
    texts: List[str] = []
    for reply in replies or []:
        text = (reply.get("text") or "").strip()
        if text:
            texts.append(text)
    return texts


def build_description_html(root_text: str, reply_texts: Sequence[str]) -> str:
    parts: List[str] = []
    root = (root_text or "").strip()
    if root:
        parts.append(_format_text_html(root))
    for text in reply_texts:
        parts.append(_format_text_html(text))
    return "<br/><br/>".join(parts)


def build_content_html(
    root_text: str,
    reply_texts: Sequence[str],
    media_urls: Sequence[str],
    youtube_embeds: Sequence[str],
    content_blocks: Optional[Sequence[Dict[str, Any]]] = None,
) -> str:
    parts: List[str] = []
    if content_blocks:
        for index, block in enumerate(content_blocks):
            if index > 0:
                parts.append(_BLOCK_DIVIDER_HTML)
            block_media = _dedupe_urls(block.get("media_urls") or [])
            if block_media:
                parts.append(_media_html(block_media))
            block_text = (block.get("text") or "").strip()
            if block_text:
                parts.append(f"<p>{xml_escape(block_text)}</p>")
    else:
        root = (root_text or "").strip()
        if root:
            parts.append(f"<p>{xml_escape(root)}</p>")
        for text in reply_texts:
            parts.append(f"<p>{xml_escape(text)}</p>")
        if media_urls:
            parts.append(_media_html(media_urls))
    if youtube_embeds:
        parts.append(_youtube_html(youtube_embeds))
    return "".join(parts)


def collect_youtube_embeds(root_text: str, reply_texts: Sequence[str]) -> List[str]:
    candidates = _extract_urls_from_text(root_text or "")
    for text in reply_texts:
        candidates.extend(_extract_urls_from_text(text))

    embeds: List[str] = []
    seen = set()
    for url in candidates:
        video_id = _youtube_id_from_url(url)
        if not video_id:
            continue
        embed_url = f"https://www.youtube.com/embed/{video_id}"
        if embed_url in seen:
            continue
        seen.add(embed_url)
        embeds.append(embed_url)
    return embeds


def build_enclosures(media_urls: Sequence[str]) -> str:
    return "".join(
        f"<enclosure url=\"{xml_escape(_normalize_media_url(media_url))}\" length=\"0\" type=\"{xml_escape(guess_mime(media_url))}\" />"
        for media_url in media_urls
    )


def build_media_contents(media_urls: Sequence[str]) -> str:
    return "".join(
        f"<media:content url=\"{xml_escape(_normalize_media_url(media_url))}\" type=\"{xml_escape(guess_mime(media_url))}\" />"
        for media_url in media_urls
    )


def build_media_players(embed_urls: Sequence[str]) -> str:
    return "".join(
        f"<media:player url=\"{xml_escape(embed_url)}\" />"
        for embed_url in embed_urls
    )


def _dedupe_urls(urls: Sequence[str]) -> List[str]:
    seen = set()
    deduped: List[str] = []
    for url in urls:
        normalized = _normalize_media_url(url)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _format_text_html(text: str) -> str:
    escaped = xml_escape(text or "")
    return escaped.replace("\n", "<br/>")


def _extract_urls_from_text(text: str) -> List[str]:
    if not text:
        return []
    urls: List[str] = []
    for token in _URL_RE.findall(text):
        cleaned = token.rstrip(".,!?;:)]}>\"'")
        if cleaned.startswith("http://") or cleaned.startswith("https://"):
            urls.append(cleaned)
    return urls


def _youtube_id_from_url(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
    except Exception:
        return None

    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = (parsed.path or "").strip()

    video_id = None
    if host == "youtu.be":
        video_id = path.lstrip("/").split("/")[0]
    elif host in {"youtube.com", "m.youtube.com", "music.youtube.com", "youtube-nocookie.com"}:
        if path == "/watch":
            video_id = (parse_qs(parsed.query).get("v") or [None])[0]
        elif path.startswith("/shorts/") or path.startswith("/live/") or path.startswith("/embed/"):
            parts = [part for part in path.split("/") if part]
            if len(parts) >= 2:
                video_id = parts[1]
    if video_id and _YOUTUBE_ID_RE.match(video_id):
        return video_id
    return None


def _media_html(media_urls: Sequence[str]) -> str:
    chunks: List[str] = []
    for url in media_urls:
        normalized = _normalize_media_url(url)
        mime = guess_mime(normalized)
        if mime.startswith("video/"):
            chunks.append(f"<video controls src=\"{xml_escape(normalized)}\"></video>")
        else:
            chunks.append(f"<img src=\"{xml_escape(normalized)}\" />")
    return "<br/>".join(chunks)


def _youtube_html(embed_urls: Sequence[str]) -> str:
    chunks: List[str] = []
    for embed_url in embed_urls:
        watch_url = embed_url.replace("/embed/", "/watch?v=")
        chunks.append(
            "<div class=\"youtube-embed\">"
            f"<p><a href=\"{xml_escape(watch_url)}\">{xml_escape(watch_url)}</a></p>"
            f"<iframe src=\"{xml_escape(embed_url)}\" loading=\"lazy\" "
            "allow=\"accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share\" "
            "allowfullscreen></iframe>"
            "</div>"
        )
    return "".join(chunks)


def _contains_any(candidates: Sequence[str], needles: Sequence[str]) -> bool:
    for c in candidates:
        if not c:
            continue
        if any(n in c for n in needles):
            return True
    return False


def _normalize_media_url(url: str) -> str:
    return html.unescape((url or "").strip())


def _unwrap_nested_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        q = parse_qs(parsed.query)
        nested = (q.get("url") or [None])[0]
        if not nested:
            return ""
        return html.unescape(unquote(str(nested))).strip()
    except Exception:
        return ""

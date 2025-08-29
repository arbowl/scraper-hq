"""RSS feed module for the aggregator."""

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
import feedparser


UA = "Mozilla/5.0 (Aggregator MWE) Python/3.x (+https://localhost)"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _to_iso_from_struct(tm_struct: Optional[time.struct_time]) -> str:
    if not tm_struct:
        return _now_iso()
    dt = datetime.fromtimestamp(
        time.mktime(tm_struct), tz=timezone.utc
    )
    return dt.replace(microsecond=0).isoformat()


async def _fetch(
    client: httpx.AsyncClient, url: str
) -> Tuple[str, Optional[bytes], Optional[str]]:
    """Fetch an RSS feed and return the URL, content, and any error."""
    try:
        r = await client.get(url, headers={"User-Agent": UA})
        r.raise_for_status()
        return url, r.content, None
    except Exception as e:
        return url, None, f"{type(e).__name__}: {e}"


def _parse_feed(raw: bytes) -> feedparser.FeedParserDict:
    """Parse an RSS feed and return a dictionary."""
    return feedparser.parse(raw)


def _host_label(url: str) -> str:
    """Get the host label from a URL."""
    try:
        return urlparse(url).hostname or "unknown"
    except Exception:
        return "unknown"


def _rows_from_entries(
    feed: feedparser.FeedParserDict, 
    source_name_fallback: str, 
    limit: int
) -> List[Dict[str, Any]]:
    """Get the rows from a feed."""
    rows: List[Dict[str, Any]] = []
    if getattr(feed, "feed", None):
        src_title = (feed.feed.get("title") or source_name_fallback).strip()
    else:
        src_title = source_name_fallback
    for entry in (feed.entries or [])[:limit]:
        title = (entry.get("title") or "").strip()
        link = entry.get("link") or ""
        ts_struct = (
            entry.get("updated_parsed") or entry.get("published_parsed")
        )
        ts_iso = _to_iso_from_struct(ts_struct)
        rows.append({
            "ts": ts_iso,
            "title": title,
            "source": src_title,
            "url": link,
        })
    return rows


async def run(config: Dict[str, Any]):
    """
    Async generator (single emission per hub interval, unless
    per_feed_stream=True).
    Config keys (all optional except template/stream are injected by hub):
      urls: list[str] | str      # one or many RSS/Atom URLs
      limit: int                 # per-feed entry limit (default 20)
      per_feed_stream: bool      # if True, emit one chunk *per feed*
      merge_sort_key: str        # when merging, key to sort by (default 'ts')
      merge_sort_dir: str        # 'desc'|'asc' (default 'desc')
      mode: 'append'|'replace'   # default 'replace' (safer for feeds)
    Emits DataChunk(s) with row schema: ts, title, source, url
    """
    urls_cfg = config.get("urls") or config.get("url")
    if isinstance(urls_cfg, str):
        urls: List[str] = [urls_cfg]
    else:
        urls = list(urls_cfg or [])
    if not urls:
        yield {
            "template": config["template"],
            "stream": config["stream"],
            "rows": [{
                "ts": _now_iso(),
                "title": "RSS config error: no urls provided",
                "source": "rss.py",
                "url": "",
            }],
            "mode": "replace",
            "ts": _now_iso(),
        }
        return
    limit = int(config.get("limit", 20))
    per_feed_stream = bool(config.get("per_feed_stream", False))
    mode = config.get("mode", "replace")
    merge_sort_key = config.get("merge_sort_key", "ts")
    merge_sort_dir = config.get("merge_sort_dir", "desc")
    timeout = httpx.Timeout(20.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"User-Agent": UA},
    ) as client:
        results = await asyncio.gather(*[_fetch(client, u) for u in urls])
    if per_feed_stream:
        for url, content, err in results:
            if err or not content:
                rows = [{
                    "ts": _now_iso(),
                    "title": f"Fetch error for {url}: {err}",
                    "source": _host_label(url),
                    "url": url,
                }]
            else:
                parsed = _parse_feed(content)
                rows = _rows_from_entries(parsed, _host_label(url), limit)
            suffix = rows[0]["source"] if rows else _host_label(url)
            stream_name = f"{config['stream']}::{suffix}"
            yield {
                "template": config["template"],
                "stream": stream_name,
                "rows": rows,
                "mode": mode,
                "ts": _now_iso(),
            }
    else:
        merged: List[Dict[str, Any]] = []
        for url, content, err in results:
            if err or not content:
                merged.append({
                    "ts": _now_iso(),
                    "title": f"Fetch error for {url}: {err}",
                    "source": _host_label(url),
                    "url": url,
                })
                continue
            parsed = _parse_feed(content)
            merged.extend(_rows_from_entries(parsed, _host_label(url), limit))
        try:
            merged.sort(
                key=lambda r: r.get(merge_sort_key, ""),
                reverse=(merge_sort_dir == "desc"),
            )
        except Exception:
            pass

        yield {
            "template": config["template"],
            "stream": config["stream"],
            "rows": merged,
            "mode": mode,
            "ts": _now_iso(),
        }

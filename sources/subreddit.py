
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx
from bs4 import BeautifulSoup


UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


async def fetch_html(url: str) -> str:
    async with httpx.AsyncClient(headers={"User-Agent": UA}, timeout=30) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.text


def parse_old_reddit(html: str, limit: int) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict[str, Any]] = []
    # old.reddit.com page uses .thing elements for posts
    for thing in soup.select("div.thing")[:limit]:
        title_el = thing.select_one("a.title")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        url = title_el.get("href")
        if url and url.startswith("/"):
            url = "https://old.reddit.com" + url
        score_el = thing.select_one("div.score.unvoted")
        score_txt = (score_el.get("title") or score_el.get_text(strip=True) if score_el else "")
        try:
            score = int(score_txt.replace("points", "").strip()) if score_txt else None
        except ValueError:
            score = None
        ts = _now_iso()
        rows.append({"ts": ts, "title": title, "score": score, "url": url})
    return rows


async def run(config: Dict[str, Any]):
    """Async generator: fetch once, yield once. Hub loops on interval.
    config keys: subreddit, listing (new|hot|top), limit
    Hub injects: template, stream
    """
    subreddit = config.get("subreddit", "python")
    listing = config.get("listing", "new")
    limit = int(config.get("limit", 25))

    url = f"https://old.reddit.com/r/{subreddit}/{listing}/"
    try:
        html = await fetch_html(url)
        rows = parse_old_reddit(html, limit=limit)
    except Exception as e:
        # on failure, emit an empty replace to note the error gracefully
        rows = [{"ts": _now_iso(), "title": f"Error: {e}", "score": None, "url": url}]

    yield {
        "template": config["template"],
        "stream": config["stream"],
        "rows": rows,
        "mode": "replace",
        "ts": _now_iso(),
    }

from __future__ import annotations

import html as html_module
import json
import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import feedparser
import requests


USER_AGENT = "OpenTraceWebMiner/1.0 (+https://github.com/OpenTrace; research)"


def _is_google_news_gate_url(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return host.endswith("news.google.com") or host.startswith("news.google.")


# Single-segment paths that are usually section fronts, not articles (when path has one segment only).
_HUB_SLUGS = frozenset(
    {
        "news",
        "world",
        "nation",
        "national",
        "global",
        "business",
        "sports",
        "entertainment",
        "lifestyle",
        "technology",
        "politics",
        "local",
        "metro",
        "regions",
        "environment",
        "opinion",
        "rss",
        "feed",
        "feeds",
        "category",
        "categories",
        "tag",
        "tags",
        "articles",
        "latest",
        "breaking",
        "home",
        "index",
    }
)


def is_site_root_or_hub_url(url: str) -> bool:
    """
    True if URL points at a site root or a single-segment hub path (e.g. /news/).
    Fetching these returns the live homepage/section mix, so body dates won't match RSS published_at.
    """
    try:
        parsed = urlparse((url or "").strip())
    except Exception:
        return True
    path = (parsed.path or "").rstrip("/")
    if not path:
        return True
    segments = [s for s in path.split("/") if s]
    if not segments:
        return True
    if len(segments) == 1 and segments[0].lower() in _HUB_SLUGS:
        return True
    return False


def _is_usable_articleish_url(url: str) -> bool:
    u = (url or "").strip()
    if not u.startswith("http"):
        return False
    if _is_google_news_gate_url(u):
        return False
    return not is_site_root_or_hub_url(u)


_HREF_RE = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.IGNORECASE)
# Plain URLs in title/summary (Google News often appends the outlet link as text, not only <a href>)
# Stop at whitespace, brackets, quotes, or trailing punctuation (strip_url_trailing_junk cleans ends).
_PLAIN_HTTP_URL_RE = re.compile(r'https?://[^\s<>"\'\[\]]+', re.IGNORECASE)


_URL_TRAIL_JUNK = frozenset({".", ",", ";", ":", "!", "?", ")", "]", "}", '"', "'", ">"})


def strip_url_trailing_junk(url: str) -> str:
    u = (url or "").strip()
    while u and u[-1] in _URL_TRAIL_JUNK:
        u = u[:-1].rstrip()
    return u


def plain_http_urls_in_text(text: str) -> list[str]:
    if not (text or "").strip():
        return []
    out: list[str] = []
    for m in _PLAIN_HTTP_URL_RE.finditer(text):
        u = strip_url_trailing_junk(m.group(0))
        if u.startswith("http"):
            out.append(u)
    return out


def _raw_entry_html_chunks(entry: Any) -> list[str]:
    """RSS/Atom HTML snippets (before plain-text conversion) for <a href=...> mining."""
    chunks: list[str] = []
    for key in ("summary", "description"):
        v = entry.get(key) if hasattr(entry, "get") else None
        if isinstance(v, str) and v.strip():
            chunks.append(v)
    c = entry.get("content") if hasattr(entry, "get") else None
    if isinstance(c, list):
        for block in c:
            if isinstance(block, dict):
                val = block.get("value")
                if isinstance(val, str) and val.strip():
                    chunks.append(val)
    return chunks


def _candidate_urls_from_html(html_fragments: list[str]) -> list[str]:
    out: list[str] = []
    for frag in html_fragments:
        for m in _HREF_RE.finditer(frag):
            u = html_module.unescape(m.group(1).strip())
            if not u.startswith("http"):
                continue
            out.append(u)
    return out


def _article_url_path_score(url: str) -> tuple[int, int]:
    """Higher = more likely a real article (deeper path, longer URL)."""
    try:
        p = urlparse(url)
        segs = len([s for s in p.path.split("/") if s])
        return (segs, len(url))
    except Exception:
        return (0, 0)


def pick_best_publisher_url(candidates: list[str]) -> str | None:
    usable = [u.strip() for u in candidates if _is_usable_articleish_url(u)]
    if not usable:
        return None
    return max(usable, key=_article_url_path_score)


def article_url_from_feed_entry(entry: Any, fallback_link: str) -> str:
    """
    Google News RSS/Atom uses news.google.com/rss/articles/... links that return a
    cookie/consent page when fetched. Prefer publisher URL from <source url="...">,
    entry.links, or <a href> inside summary/content HTML (Google often embeds the real story link).
    """
    fb = (fallback_link or "").strip()
    if fb and not _is_google_news_gate_url(fb):
        return fb

    candidates: list[str] = []

    source = entry.get("source") if hasattr(entry, "get") else None
    if source is not None and not isinstance(source, dict):
        try:
            source = dict(source)
        except Exception:
            source = None
    if isinstance(source, dict):
        for key in ("href", "url"):
            u = str(source.get(key) or "").strip()
            if u.startswith("http") and not _is_google_news_gate_url(u):
                candidates.append(u)

    links = entry.get("links") if hasattr(entry, "get") else None
    if isinstance(links, list):
        for link in links:
            if not isinstance(link, dict):
                try:
                    link = dict(link)
                except Exception:
                    continue
            href = str(link.get("href") or "").strip()
            if href.startswith("http") and not _is_google_news_gate_url(href):
                candidates.append(href)

    candidates.extend(_candidate_urls_from_html(_raw_entry_html_chunks(entry)))

    title_raw = str(entry.get("title") or "")
    candidates.extend(plain_http_urls_in_text(title_raw))
    for frag in _raw_entry_html_chunks(entry):
        candidates.extend(plain_http_urls_in_text(frag))

    best = pick_best_publisher_url(candidates)
    if best:
        return best

    return fb


def resolve_rss_item_fetch_url(item: RssItem) -> str:
    """
    If the RSS item still points at a Google News gate URL, try to recover a direct
    publisher URL from title, plain summary, and raw-RSS URL hints.
    """
    u = (item.url or "").strip()
    if not _is_google_news_gate_url(u):
        return u
    blob = f"{item.title}\n{item.summary}"
    found: list[str] = list(plain_http_urls_in_text(blob)) + list(item.extra_fetch_url_hints)
    best = pick_best_publisher_url(found)
    return best if best else u


@dataclass
class RssItem:
    url: str
    title: str
    summary: str
    published: date | None
    feed_name: str
    # URLs seen in raw RSS HTML before stripping tags (Google News often only links in HTML).
    extra_fetch_url_hints: tuple[str, ...] = ()


def load_country_feeds(feeds_json_path: Path) -> dict[str, list[dict[str, str]]]:
    raw = json.loads(feeds_json_path.read_text(encoding="utf-8"))
    out: dict[str, list[dict[str, str]]] = {}
    for country, feeds in raw.items():
        if str(country).startswith("_"):
            continue
        if not isinstance(feeds, list):
            continue
        cleaned: list[dict[str, str]] = []
        for f in feeds:
            if not isinstance(f, dict):
                continue
            url = str(f.get("url", "")).strip()
            name = str(f.get("name", urlparse(url).netloc or "feed")).strip()
            if url:
                cleaned.append({"name": name, "url": url})
        out[str(country)] = cleaned
    return out


def _struct_time_to_date(st: time.struct_time | None) -> date | None:
    if st is None:
        return None
    try:
        return date(st.tm_year, st.tm_mon, st.tm_mday)
    except Exception:
        return None


def _coerce_parsed_time(value: Any) -> time.struct_time | None:
    """
    feedparser entry fields like published_parsed are struct_time at runtime, but stubs
    may include unions (e.g. list). Normalize to struct_time | None for type-safe use.
    """
    if value is None:
        return None
    if isinstance(value, time.struct_time):
        return value
    # Some feeds / versions use a 9-tuple compatible with struct_time.
    if isinstance(value, tuple) and len(value) == 9:
        try:
            return time.struct_time(tuple(int(x) for x in value))
        except Exception:
            return None
    if isinstance(value, list) and value:
        return _coerce_parsed_time(value[0])
    return None


def fetch_feed_entries(feed_url: str, feed_name: str, timeout: float = 30.0) -> list[RssItem]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*"}
    resp = requests.get(feed_url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    parsed = feedparser.parse(resp.content)
    items: list[RssItem] = []
    for entry in parsed.entries or []:
        _link = entry.get("link") or entry.get("id") or ""
        link = _link.strip() if isinstance(_link, str) else str(_link).strip()
        link = article_url_from_feed_entry(entry, link)
        if not link:
            continue
        _title = entry.get("title") or ""
        title = _title.strip() if isinstance(_title, str) else str(_title).strip()
        _summary = entry.get("summary") or entry.get("description") or ""
        summary_raw = _summary.strip() if isinstance(_summary, str) else str(_summary).strip()
        hints: list[str] = []
        hints.extend(plain_http_urls_in_text(title))
        hints.extend(plain_http_urls_in_text(summary_raw))
        for frag in _raw_entry_html_chunks(entry):
            hints.extend(plain_http_urls_in_text(frag))
        hints.extend(_candidate_urls_from_html(_raw_entry_html_chunks(entry)))
        hint_tuple = tuple(dict.fromkeys(h for h in hints if h.startswith("http")))
        summary = summary_raw
        if "<" in summary and ">" in summary:
            from ml.web_data_mining.agents.html_text import html_to_plain_text

            summary = html_to_plain_text(summary)
        pub = None
        pub_raw = entry.get("published_parsed")
        if pub_raw:
            pub = _struct_time_to_date(_coerce_parsed_time(pub_raw))
        if pub is None:
            upd_raw = entry.get("updated_parsed")
            if upd_raw:
                pub = _struct_time_to_date(_coerce_parsed_time(upd_raw))
        items.append(
            RssItem(
                url=link,
                title=title,
                summary=summary,
                published=pub,
                feed_name=feed_name,
                extra_fetch_url_hints=hint_tuple,
            )
        )
    return items


def item_in_date_window(item: RssItem, start: date | None, end: date | None) -> bool:
    if start is None or end is None:
        return True
    if item.published is None:
        # Keep items with unknown date (many feeds omit it); caller can tighten policy later.
        return True
    return start <= item.published <= end

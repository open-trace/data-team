"""
Agentic URL discovery stage (Tavily Search) for rss|tavily|hybrid modes.

This is discovery-only: results are converted to RssItem-like candidates and then
the existing fetch/extract/storage pipeline decides what survives.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from ml.web_data_mining.agents.rss_discovery import RssItem
from ml.web_data_mining.agentic.tavily_tools import is_tavily_configured, tavily_search_news


def _parse_pub_date(raw: Any) -> date | None:
    s = str(raw or "").strip()
    if not s:
        return None
    # common Tavily shapes: YYYY-MM-DD or datetime ISO
    try:
        if len(s) >= 10:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None
    return None


def discover_items_with_tavily(
    *,
    country: str,
    domains: list[str],
    start_date: date | None,
    end_date: date | None,
    max_results_per_domain: int,
) -> list[RssItem]:
    """
    Discover candidate news URLs via Tavily Search.

    Returns RssItem-compatible objects so orchestrator can reuse existing scoring/fetch logic.
    """
    if not is_tavily_configured():
        return []

    out: list[RssItem] = []
    seen_urls: set[str] = set()
    s_iso = start_date.isoformat() if start_date else None
    e_iso = end_date.isoformat() if end_date else None

    for dom in domains:
        q = f"{country} {dom} news"
        _text, rows, err = tavily_search_news(
            q,
            max_results=max_results_per_domain,
            search_depth="advanced",
            start_date=s_iso,
            end_date=e_iso,
        )
        if err:
            continue
        for row in rows:
            url = str(row.get("url") or "").strip()
            if not url or not url.startswith("http") or url in seen_urls:
                continue
            seen_urls.add(url)
            title = str(row.get("title") or "").strip() or f"{country} {dom}"
            summary = str(row.get("content") or "").strip()
            pub = _parse_pub_date(row.get("published_date") or row.get("date") or row.get("published"))
            out.append(
                RssItem(
                    url=url,
                    title=title,
                    summary=summary,
                    published=pub,
                    feed_name=f"Tavily discovery — {dom}",
                    extra_fetch_url_hints=(),
                )
            )

    return out

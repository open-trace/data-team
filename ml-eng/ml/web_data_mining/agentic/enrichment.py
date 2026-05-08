"""
Bridge RSS pipeline → Tavily when local fetch/summary is thin.

Does not replace fetch_and_extract; runs only when enabled and body is still short.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from ml.web_data_mining.agentic.tavily_tools import is_tavily_configured, tavily_extract_urls, tavily_search_news

if TYPE_CHECKING:
    from ml.web_data_mining.agents.rss_discovery import RssItem


def _publisher_candidate_urls(item: RssItem, fetch_url: str, phase1_fetch_url: str) -> list[str]:
    out: list[str] = []
    for u in (fetch_url, phase1_fetch_url, item.url):
        u = (u or "").strip()
        if u.startswith("http") and "news.google.com/rss/articles" not in u.lower():
            if u not in out:
                out.append(u)
    return out


def try_enrich_with_tavily(
    *,
    item: RssItem,
    body: str,
    page_title: str,
    fetch_url: str,
    phase1_fetch_url: str,
    min_chars: int,
    max_search_results: int = 3,
    try_extract: bool = True,
    headline_only: bool = False,
    country: str = "",
    domain: str = "",
    use_langgraph: bool = False,
    graph_recursion_limit: int = 28,
) -> tuple[str, str, str | None]:
    """
    If body is still short (or looks like headline-only), try Tavily Extract on publisher
    URLs then Tavily Search (news) on a query from the RSS title/summary.

    Returns (body, page_title, body_source_tag or None).
    body_source_tag: 'tavily_extract' | 'tavily_search' when Tavily supplied new text.
    """
    b = (body or "").strip()
    title = (page_title or item.title or "").strip()

    if len(b) >= min_chars and not headline_only:
        return body, page_title, None

    if not is_tavily_configured():
        return body, page_title, None

    # LangGraph path (deep research style): LLM + Tavily tools + compress — needs OPENAI_API_KEY etc.
    if use_langgraph:
        try:
            from ml.web_data_mining.agentic.mining_research_graph import run_mining_research_for_article
        except ImportError:
            pass
        else:
            res = run_mining_research_for_article(
                country=country or "",
                domain=domain or "",
                rss_title=item.title or page_title,
                rss_summary=(item.summary or "").strip(),
                rss_url=item.url,
                candidate_urls=_publisher_candidate_urls(item, fetch_url, phase1_fetch_url),
                existing_body_preview=body,
                min_chars=min_chars,
                recursion_limit=graph_recursion_limit,
            )
            if res.ok and res.body:
                return res.body.strip(), (page_title or item.title or "").strip(), "tavily_langgraph"

    # 1) Linear path — Extract: any non-Google HTTP URL we saw during the two-phase fetch
    candidates = _publisher_candidate_urls(item, fetch_url, phase1_fetch_url)

    if try_extract and candidates:
        extracted, err = tavily_extract_urls(candidates)
        if extracted and len(extracted.strip()) >= min_chars:
            return extracted.strip(), title, "tavily_extract"
        if err:
            pass  # fall through to search

    # 2) Search: natural-language query from RSS (country + agrifood context)
    q_parts = [title, (item.summary or "").strip()[:400]]
    query = " ".join(x for x in q_parts if x)
    if len(query) < 12:
        return body, page_title, None

    snippet, _results, s_err = tavily_search_news(query, max_results=max_search_results)
    if snippet and len(snippet.strip()) >= min_chars:
        return snippet.strip(), title, "tavily_search"

    return body, page_title, None

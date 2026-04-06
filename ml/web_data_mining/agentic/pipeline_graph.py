from __future__ import annotations

from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from ml.web_data_mining.agents.dedupe_cluster import (
    cluster_id,
    content_hash,
    dedupe_id,
    normalize_url_for_dedupe,
    title_similarity,
)
from ml.web_data_mining.agents.domain_agent import DomainAgentRegistry, agricultural_context_signals
from ml.web_data_mining.agents.fetch_extract import (
    fetch_and_extract,
    is_google_consent_or_gate_page,
)
from ml.web_data_mining.agents.orchestrator import (
    _body_is_mostly_headline_only,
    _date_window,
    _feed_fetch_jobs,
    _prefer_item_title,
)
from ml.web_data_mining.agents.rss_discovery import (
    RssItem,
    fetch_feed_entries,
    is_site_root_or_hub_url,
    item_in_date_window,
    load_country_feeds,
)
from ml.web_data_mining.agents.storage_txt import article_id_from_url, write_news_txt
from ml.web_data_mining.agents.temporal import pick_published_at, utc_now_iso
from ml.web_data_mining.agentic.state_pipeline import MiningPipelineState
from ml.web_data_mining.schemas import RunParams


def _dbg(params: RunParams, msg: str) -> None:
    if getattr(params, "debug_pipeline_graph", False):
        print(f"[pipeline-graph][debug] {msg}")


def _trace(state: MiningPipelineState, node: str) -> dict[str, Any]:
    _dbg(state["params"], f"node={node} idx={int(state.get('candidate_idx', 0))}")
    return {"node_trace": [*state.get("node_trace", []), node]}


def load_feeds(state: MiningPipelineState) -> dict[str, Any]:
    params = state["params"]
    mode = getattr(params, "discovery_mode", "rss")
    include_rss = mode in {"rss", "hybrid"} or (
        mode == "tavily" and getattr(params, "include_feeds_with_tavily", True)
    )
    feeds_by_country: dict[str, list[dict[str, str]]] = {}
    if include_rss:
        feeds_path = Path(params.feeds_path or "")
        if not feeds_path.exists():
            return {
                **_trace(state, "load_feeds"),
                "exit_code": 1,
                "stop_run": True,
                "error": f"Feeds file not found: {feeds_path}",
            }
        feeds_by_country = load_country_feeds(feeds_path)
    return {
        **_trace(state, "load_feeds"),
        "feeds_by_country": feeds_by_country,
        "include_rss": include_rss,
    }


def discover_candidates(state: MiningPipelineState) -> dict[str, Any]:
    params = state["params"]
    mode = getattr(params, "discovery_mode", "rss")
    include_rss = bool(state.get("include_rss", False))
    feeds_by_country: dict[str, list[dict[str, str]]] = state.get("feeds_by_country", {})
    domain_registry = DomainAgentRegistry(active_domains=params.domains)
    start_d, end_d = _date_window(params)

    candidates: list[dict[str, Any]] = []
    for country in params.countries:
        if include_rss:
            feeds = feeds_by_country.get(country, [])
            for feed in feeds:
                jobs = _feed_fetch_jobs(feed, params)
                for fetch_url, fetch_name in jobs:
                    try:
                        entries = fetch_feed_entries(fetch_url, fetch_name)
                    except Exception:
                        continue
                    for item in entries:
                        if not item_in_date_window(item, start_d, end_d):
                            continue
                        dom, score = domain_registry.best_domain(f"{item.title} {item.summary}")
                        if score < 1 or dom not in params.domains:
                            continue
                        candidates.append({"country": country, "item": item, "domain": dom, "score": score})

        if mode in {"tavily", "hybrid"}:
            try:
                from ml.web_data_mining.agentic.url_discovery import discover_items_with_tavily

                discovered = discover_items_with_tavily(
                    country=country,
                    domains=params.domains,
                    start_date=start_d,
                    end_date=end_d,
                    max_results_per_domain=getattr(params, "tavily_discovery_max_results", 5),
                )
                min_score = int(getattr(params, "tavily_discovery_min_domain_score", 1))
                for item in discovered:
                    dom, score = domain_registry.best_domain(f"{item.title} {item.summary}")
                    if score < min_score or dom not in params.domains:
                        continue
                    candidates.append({"country": country, "item": item, "domain": dom, "score": score})
            except Exception:
                pass

    _dbg(params, f"discover_candidates total={len(candidates)}")
    return {**_trace(state, "discover_candidates"), "candidates": candidates}


def dedupe_rank(state: MiningPipelineState) -> dict[str, Any]:
    params = state["params"]
    by_url: dict[str, dict[str, Any]] = {}
    for c in state.get("candidates", []):
        item: RssItem = c["item"]
        prev = by_url.get(item.url)
        if prev is None or c["score"] > prev["score"]:
            by_url[item.url] = c
    ordered = sorted(by_url.values(), key=lambda x: x["score"], reverse=True)
    ranked = ordered[: params.max_urls_per_country]
    _dbg(params, f"dedupe_rank unique={len(ordered)} sliced={len(ranked)}")
    return {
        **_trace(state, "dedupe_rank"),
        "ranked_candidates": ranked,
        "candidate_idx": 0,
        "saved_count": 0,
    }


def fetch_phase1(state: MiningPipelineState) -> dict[str, Any]:
    idx = int(state.get("candidate_idx", 0))
    candidates: list[dict[str, Any]] = state.get("ranked_candidates", [])
    if idx >= len(candidates):
        _dbg(state["params"], "fetch_phase1 done=true")
        return {**_trace(state, "fetch_phase1"), "done": True}
    current = candidates[idx]
    item: RssItem = current["item"]
    if is_site_root_or_hub_url(item.url):
        _dbg(state["params"], f"skip hub url={item.url[:120]}")
        return {**_trace(state, "fetch_phase1"), "skip_current": True}
    try:
        page_title, body, fetch_url, phase1_fetch_url, extract_meta = fetch_and_extract(item)
    except Exception:
        _dbg(state["params"], f"fetch failed url={item.url[:120]}")
        return {**_trace(state, "fetch_phase1"), "skip_current": True}
    return {
        **_trace(state, "fetch_phase1"),
        "done": False,
        "current": current,
        "page_title": page_title,
        "body": body,
        "fetch_url": fetch_url,
        "phase1_fetch_url": phase1_fetch_url,
        "extract_meta": extract_meta,
    }


def resolve_article_url(state: MiningPipelineState) -> dict[str, Any]:
    return _trace(state, "resolve_article_url")


def fetch_phase2(state: MiningPipelineState) -> dict[str, Any]:
    return _trace(state, "fetch_phase2")


def clean_extract(state: MiningPipelineState) -> dict[str, Any]:
    params = state["params"]
    if state.get("done") or state.get("skip_current"):
        return _trace(state, "clean_extract")
    current_obj = state.get("current")
    if not current_obj:
        return {**_trace(state, "clean_extract"), "skip_current": True}
    item: RssItem = current_obj["item"]
    page_title = state.get("page_title", "")
    body = (state.get("body") or "").strip()
    url = item.url
    body_source = "fetched_html"

    def _looks_like_binary_text(text: str, scan_chars: int = 800) -> bool:
        if not text:
            return False
        head = text.lstrip()[:16]
        if head.startswith("\ufffdPNG") or head.startswith("�PNG"):
            return True
        scan = text[:scan_chars]
        ctrl = sum(1 for ch in scan if (ord(ch) < 32 and ch not in "\n\r\t"))
        rep = scan.count("\ufffd")
        return ctrl >= 10 or rep >= 40

    # Guardrail: if the "body" is actually raw PDF bytes decoded as text, don't save it.
    # This can happen when a publisher mislabels content-type, pypdf is missing, or extraction fails.
    if body.lstrip().startswith("%PDF-"):
        _dbg(params, f"skip pdf-bytes body url={url[:120]}")
        return {**_trace(state, "clean_extract"), "skip_current": True}
    if _looks_like_binary_text(body):
        _dbg(params, f"skip binary-bytes body url={url[:120]}")
        return {**_trace(state, "clean_extract"), "skip_current": True}

    if is_google_consent_or_gate_page(page_title, body):
        summary = (item.summary or "").strip()
        if len(summary) >= params.min_rss_summary_chars:
            body = summary
            body_source = "rss_summary_google_gate"
            page_title = item.title
        elif not params.tavily_enrich:
            _dbg(params, f"skip google-gate short summary url={url[:120]}")
            return {**_trace(state, "clean_extract"), "skip_current": True}
    elif len(body) < params.min_article_chars:
        summary = (item.summary or "").strip()
        if len(summary) >= params.min_rss_summary_chars and ("news.google.com" in url or len(summary) >= params.min_article_chars):
            body = summary
            body_source = "rss_summary_fallback"
            page_title = _prefer_item_title(page_title, item.title)
        elif not params.tavily_enrich:
            _dbg(params, f"skip thin body without enrichment url={url[:120]}")
            return {**_trace(state, "clean_extract"), "skip_current": True}

    page_title = _prefer_item_title(page_title, item.title)
    return {**_trace(state, "clean_extract"), "page_title": page_title, "body": body, "body_source": body_source}


def extract_dates(state: MiningPipelineState) -> dict[str, Any]:
    return _trace(state, "extract_dates")


def normalize_dates(state: MiningPipelineState) -> dict[str, Any]:
    return _trace(state, "normalize_dates")


def agrifood_gate(state: MiningPipelineState) -> dict[str, Any]:
    return _trace(state, "agrifood_gate")


def domain_score(state: MiningPipelineState) -> dict[str, Any]:
    return _trace(state, "domain_score")


def enrichment_decision(state: MiningPipelineState) -> dict[str, Any]:
    if state.get("done") or state.get("skip_current"):
        return _trace(state, "enrichment_decision")
    params = state["params"]
    body = (state.get("body") or "").strip()
    current_obj = state.get("current")
    if not current_obj:
        return {**_trace(state, "enrichment_decision"), "skip_current": True}
    final_title = state.get("page_title") or current_obj["item"].title
    thin = len(body) < params.min_article_chars
    ho = _body_is_mostly_headline_only(final_title, body)
    return {**_trace(state, "enrichment_decision"), "needs_enrichment": bool(params.tavily_enrich and (thin or ho))}


def tavily_enrich(state: MiningPipelineState) -> dict[str, Any]:
    if not state.get("needs_enrichment"):
        return _trace(state, "tavily_enrich")
    params = state["params"]
    current = state.get("current")
    if not current:
        return {**_trace(state, "tavily_enrich"), "skip_current": True}
    item: RssItem = current["item"]
    body = state.get("body", "")
    final_title = state.get("page_title") or item.title
    ho = _body_is_mostly_headline_only(final_title, body)
    try:
        from ml.web_data_mining.agentic.enrichment import try_enrich_with_tavily

        nb, nt, tag = try_enrich_with_tavily(
            item=item,
            body=body,
            page_title=final_title,
            fetch_url=state.get("fetch_url", ""),
            phase1_fetch_url=state.get("phase1_fetch_url", ""),
            min_chars=params.min_article_chars,
            max_search_results=params.tavily_max_search_results,
            try_extract=params.tavily_extract_first,
            headline_only=ho,
            country=current["country"],
            domain=current["domain"],
            use_langgraph=getattr(params, "tavily_use_langgraph", False),
            graph_recursion_limit=getattr(params, "tavily_graph_recursion_limit", 28),
        )
        if tag:
            _dbg(params, f"tavily_enrich success tag={tag}")
            return {**_trace(state, "tavily_enrich"), "body": nb, "page_title": nt or final_title, "body_source": tag}
    except Exception:
        pass
    return _trace(state, "tavily_enrich")


def optional_deep_research(state: MiningPipelineState) -> dict[str, Any]:
    return _trace(state, "optional_deep_research")


def merge_enrichment(state: MiningPipelineState) -> dict[str, Any]:
    return _trace(state, "merge_enrichment")


def store_article(state: MiningPipelineState) -> dict[str, Any]:
    out = _trace(state, "store_article")
    if state.get("done"):
        return {**out, "exit_code": 0}

    idx = int(state.get("candidate_idx", 0))
    candidates: list[dict[str, Any]] = state.get("ranked_candidates", [])
    if idx >= len(candidates):
        return {**out, "done": True, "exit_code": 0}
    if state.get("skip_current"):
        return {**out, "candidate_idx": idx + 1, "skip_current": False}

    params = state["params"]
    current = state.get("current")
    if not current:
        return {**out, "candidate_idx": idx + 1, "skip_current": False}
    item: RssItem = current["item"]
    country = current["country"]
    dom = current["domain"]
    score = current["score"]
    body = (state.get("body") or "").strip()
    final_title = (state.get("page_title") or item.title).strip()
    body_source = state.get("body_source", "fetched_html")
    url = item.url

    if len(body) < params.min_article_chars:
        _dbg(params, f"skip store short-body len={len(body)} url={url[:120]}")
        return {**out, "candidate_idx": idx + 1}
    if _body_is_mostly_headline_only(final_title, body) and len(body) < params.min_article_chars:
        return {**out, "candidate_idx": idx + 1}
    if body_source == "tavily_search" and not getattr(params, "tavily_allow_search_snippet_save", False):
        _dbg(params, f"skip store tavily_search snippets disabled url={url[:120]}")
        return {**out, "candidate_idx": idx + 1}

    full_blob = f"{final_title}\n{body}"
    agrifood_signals = agricultural_context_signals(full_blob)
    if not agrifood_signals:
        _dbg(params, f"skip non-agrifood url={url[:120]}")
        return {**out, "candidate_idx": idx + 1}

    domain_registry = DomainAgentRegistry(active_domains=params.domains)
    domain_scores = domain_registry.scores(full_blob)
    domain_labels = domain_registry.ranked_labels(full_blob)
    best_domain = domain_labels[0] if domain_labels else dom
    if best_domain not in params.domains:
        _dbg(params, f"skip domain not allowed domain={best_domain!r} url={url[:120]}")
        return {**out, "candidate_idx": idx + 1}

    ingest = utc_now_iso()
    extract_meta = state.get("extract_meta", {})
    extracted_at = (extract_meta.get("extracted_at") or utc_now_iso()) if isinstance(extract_meta, dict) else utc_now_iso()
    html_published = extract_meta.get("html_published_at") if isinstance(extract_meta, dict) else None
    article_updated_at = extract_meta.get("article_updated_at") if isinstance(extract_meta, dict) else None
    published = pick_published_at(
        rss_date=item.published,
        html_published_iso=html_published,
        tavily_published_iso=None,
        inferred_iso=ingest,
    )

    seen_norm_urls: dict[str, str] = state.get("seen_norm_urls", {})
    canonical_title: dict[str, tuple[str, str]] = state.get("canonical_title", {})
    canonical_body_hash: dict[str, str] = state.get("canonical_body_hash", {})
    fetch_url = state.get("fetch_url", url)
    norm_url = normalize_url_for_dedupe(fetch_url or url)
    host = urlparse(fetch_url or url).netloc.lower()
    this_dedupe_id = dedupe_id(final_title, host)
    this_cluster_id = cluster_id(final_title, country, dom, published.published_at)
    this_body_hash = content_hash(body)
    aid = article_id_from_url(url)
    is_dup = False
    duplicate_of: str | None = None

    if norm_url in seen_norm_urls:
        is_dup = True
        duplicate_of = seen_norm_urls[norm_url]
    elif this_dedupe_id in canonical_title:
        prev_title, prev_id = canonical_title[this_dedupe_id]
        if title_similarity(prev_title, final_title) >= 0.9:
            is_dup = True
            duplicate_of = prev_id
    elif this_body_hash in canonical_body_hash:
        is_dup = True
        duplicate_of = canonical_body_hash[this_body_hash]
    if not is_dup:
        seen_norm_urls[norm_url] = aid
        canonical_title[this_dedupe_id] = (final_title, aid)
        canonical_body_hash[this_body_hash] = aid

    output_root = Path(params.output_dir).resolve()
    meta = {
        "id": aid,
        "url": url,
        "rss_url": url,
        "title": final_title[:500],
        "country": country,
        "domain": best_domain,
        "domain_score": domain_scores.get(best_domain, score),
        "info_type": "news_article",
        "published_at": published.published_at,
        "published_at_source": published.published_at_source,
        "published_at_confidence": published.published_at_confidence,
        "published_at_raw": published.published_at_raw,
        "html_published_at": html_published,
        "html_published_raw": extract_meta.get("html_published_raw") if isinstance(extract_meta, dict) else None,
        "article_updated_at": article_updated_at,
        "html_updated_raw": extract_meta.get("html_updated_raw") if isinstance(extract_meta, dict) else None,
        "ingested_at": ingest,
        "extracted_at": extracted_at,
        "feed_name": item.feed_name,
        "source": "rss",
        "body_source": body_source,
        "phase1_fetch_url": state.get("phase1_fetch_url"),
        "article_fetch_url": fetch_url,
        "enriched": str(body_source).startswith("tavily_"),
        "enrichment_used": body_source if str(body_source).startswith("tavily_") else None,
        "dedupe_id": this_dedupe_id,
        "cluster_id": this_cluster_id,
        "is_duplicate": is_dup,
        "duplicate_of": duplicate_of,
        "agrifood_signal": True,
        "agrifood_signals": agrifood_signals[:25],
        "domain_scores": domain_scores,
        "domain_labels": domain_labels,
        "best_domain": best_domain,
        "pipeline_nodes_executed": state.get("node_trace", []) + ["store_article"],
    }
    if fetch_url != url:
        meta["fetch_url"] = fetch_url

    try:
        path = write_news_txt(output_root, country, meta, body)
        saved_count = int(state.get("saved_count", 0)) + 1
        _dbg(params, f"saved path={path}")
    except Exception:
        saved_count = int(state.get("saved_count", 0))
        _dbg(params, f"storage failed url={url[:120]}")

    return {
        **out,
        "candidate_idx": idx + 1,
        "saved_count": saved_count,
        "seen_norm_urls": seen_norm_urls,
        "canonical_title": canonical_title,
        "canonical_body_hash": canonical_body_hash,
        "skip_current": False,
    }


def _choose_enrichment_path(state: MiningPipelineState) -> Literal["store_article", "tavily_enrich"]:
    if state.get("done") or state.get("skip_current"):
        return "store_article"
    if state.get("needs_enrichment"):
        return "tavily_enrich"
    return "store_article"


def _choose_research_path(state: MiningPipelineState) -> Literal["optional_deep_research", "merge_enrichment"]:
    if state["params"].tavily_use_langgraph:
        return "optional_deep_research"
    return "merge_enrichment"


def _choose_loop_or_end(state: MiningPipelineState) -> Literal["fetch_phase1", "__end__"]:
    if state.get("stop_run"):
        return "__end__"
    idx = int(state.get("candidate_idx", 0))
    total = len(state.get("ranked_candidates", []))
    if idx < total:
        return "fetch_phase1"
    return "__end__"


def run_pipeline_with_langgraph(params: RunParams) -> int:
    try:
        from langgraph.graph import END, START, StateGraph
    except Exception as exc:
        raise RuntimeError(
            "LangGraph execution requested but dependencies are missing. "
            "Install: pip install -r ml/web_data_mining/requirements-agent-graph.txt"
        ) from exc

    g = StateGraph(MiningPipelineState)
    g.add_node("load_feeds", load_feeds)
    g.add_node("discover_candidates", discover_candidates)
    g.add_node("dedupe_rank", dedupe_rank)
    g.add_node("fetch_phase1", fetch_phase1)
    g.add_node("resolve_article_url", resolve_article_url)
    g.add_node("fetch_phase2", fetch_phase2)
    g.add_node("clean_extract", clean_extract)
    g.add_node("extract_dates", extract_dates)
    g.add_node("normalize_dates", normalize_dates)
    g.add_node("agrifood_gate", agrifood_gate)
    g.add_node("domain_score", domain_score)
    g.add_node("enrichment_decision", enrichment_decision)
    g.add_node("tavily_enrich", tavily_enrich)
    g.add_node("optional_deep_research", optional_deep_research)
    g.add_node("merge_enrichment", merge_enrichment)
    g.add_node("store_article", store_article)

    g.add_edge(START, "load_feeds")
    g.add_edge("load_feeds", "discover_candidates")
    g.add_edge("discover_candidates", "dedupe_rank")
    g.add_edge("dedupe_rank", "fetch_phase1")
    g.add_edge("fetch_phase1", "resolve_article_url")
    g.add_edge("resolve_article_url", "fetch_phase2")
    g.add_edge("fetch_phase2", "clean_extract")
    g.add_edge("clean_extract", "extract_dates")
    g.add_edge("extract_dates", "normalize_dates")
    g.add_edge("normalize_dates", "agrifood_gate")
    g.add_edge("agrifood_gate", "domain_score")
    g.add_edge("domain_score", "enrichment_decision")
    g.add_conditional_edges(
        "enrichment_decision",
        _choose_enrichment_path,
        {"store_article": "store_article", "tavily_enrich": "tavily_enrich"},
    )
    g.add_conditional_edges(
        "tavily_enrich",
        _choose_research_path,
        {"optional_deep_research": "optional_deep_research", "merge_enrichment": "merge_enrichment"},
    )
    g.add_edge("optional_deep_research", "merge_enrichment")
    g.add_edge("merge_enrichment", "store_article")
    g.add_conditional_edges(
        "store_article",
        _choose_loop_or_end,
        {"fetch_phase1": "fetch_phase1", "__end__": END},
    )

    graph = g.compile()
    # Pipeline graph loops across many candidates; default recursion limit (25) is too low.
    out = graph.invoke(
        {"params": params, "node_trace": [], "exit_code": 1},
        config={"recursion_limit": 2000},
    )
    trace = out.get("node_trace", [])
    print(f"[pipeline-graph] executed nodes: {trace}")
    print(f"[pipeline-graph] saved_count={int(out.get('saved_count', 0))}")
    if out.get("error"):
        print(f"[pipeline-graph] error={out.get('error')}")
    return int(out.get("exit_code", 0))

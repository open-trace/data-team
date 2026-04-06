from __future__ import annotations

import re
import time
import traceback
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

from ml.web_data_mining.agents.domain_agent import DomainAgentRegistry, agricultural_context_signals
from ml.web_data_mining.agents.dedupe_cluster import (
    cluster_id,
    content_hash,
    dedupe_id,
    normalize_url_for_dedupe,
    title_similarity,
)
from ml.web_data_mining.agents.fetch_extract import fetch_and_extract, is_google_consent_or_gate_page
from ml.web_data_mining.agents.rss_discovery import (
    RssItem,
    fetch_feed_entries,
    is_site_root_or_hub_url,
    item_in_date_window,
    load_country_feeds,
)
from ml.web_data_mining.agents.storage_txt import article_id_from_url, write_news_txt
from ml.web_data_mining.agents.temporal import pick_published_at, utc_now_iso
from ml.web_data_mining.google_news_slice import (
    count_slice_days,
    expand_google_news_rss_urls,
    is_google_news_search_rss,
    slice_range_from_params,
)
from ml.web_data_mining.schemas import RunParams


def _date_window(params: RunParams) -> tuple[date | None, date | None]:
    if params.start_date and params.end_date:
        return params.start_date, params.end_date
    assert params.start_year is not None and params.end_year is not None
    return date(params.start_year, 1, 1), date(params.end_year, 12, 31)


def _norm_headline(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower().strip())


_BAD_FETCH_TITLES = frozenset(
    {
        "google news",
        "news",
        "home",
        "sign in",
        "before you continue",
        "",
        "error 400 (bad request)!!1",
        "error 400 (bad request)",
    }
)


def _prefer_item_title(page_title: str, item_title: str) -> str:
    pt = (page_title or "").strip()
    it = (item_title or "").strip()
    if not it:
        return pt
    pl = pt.lower()
    if not pt or pl in _BAD_FETCH_TITLES or len(pt) < 12:
        return it
    if "error 400" in pl or "bad request" in pl:
        return it
    return pt


def _body_is_mostly_headline_only(title: str, body: str) -> bool:
    """True when body adds little beyond the title (common for Google RSS summary fallback)."""
    t = _norm_headline(title)
    b = _norm_headline(body)
    if not b:
        return True
    if not t:
        return len(b) < 120
    if b == t:
        return True
    if t in b and len(b) <= len(t) + 40:
        return True
    if b in t and len(t) <= len(b) + 40:
        return True
    return False


def _short_body_skip_hint(params: RunParams) -> str:
    """Explain what was already tried when we skip for thin body."""
    if params.tavily_enrich:
        return "after RSS fetch + Tavily enrichment attempt"
    return (
        "after RSS fetch only — use --tavily-enrich (and TAVILY_API_KEY) to recover many Google News items"
    )


def _feed_fetch_jobs(feed: dict[str, str], params: RunParams) -> list[tuple[str, str]]:
    """Return (fetch_url, feed_display_name) jobs; may be one URL or one per day for Google News."""
    url = feed["url"]
    name = feed["name"]
    if not params.google_news_daily_slice or not is_google_news_search_rss(url):
        return [(url, name)]
    sr, er = slice_range_from_params(
        params.start_date,
        params.end_date,
        params.start_year,
        params.end_year,
    )
    expanded = expand_google_news_rss_urls(url, sr, er)
    return [(u, f"{name} {suffix}") for u, suffix in expanded]


class RssMiningOrchestrator:
    """
    Coordinates RSS discovery (per country), domain specialist scoring, fetch/extract, and TXT storage.
    """

    def __init__(
        self,
        min_domain_score: int = 1,
        min_article_chars: int = 200,
        min_rss_summary_chars: int = 80,
    ) -> None:
        self.min_domain_score = min_domain_score
        self.min_article_chars = min_article_chars
        self.min_rss_summary_chars = min_rss_summary_chars

    def plan(self, params: RunParams) -> None:
        mode = getattr(params, "discovery_mode", "rss")
        include_rss = mode in {"rss", "hybrid"} or (
            mode == "tavily" and getattr(params, "include_feeds_with_tavily", True)
        )
        feeds_by_country: dict[str, list[dict[str, str]]] = {}
        if include_rss:
            feeds_path = Path(params.feeds_path or "")
            if not feeds_path.exists():
                print(f"[orchestrator] Feeds file not found: {feeds_path}")
                return
            feeds_by_country = load_country_feeds(feeds_path)
        start_d, end_d = _date_window(params)
        print(f"[orchestrator] Date window: {start_d} .. {end_d}")
        print(f"[orchestrator] Discovery mode: {mode}")
        if params.google_news_daily_slice:
            sr, er = slice_range_from_params(
                params.start_date,
                params.end_date,
                params.start_year,
                params.end_year,
            )
            nd = count_slice_days(sr, er)
            print(
                f"[orchestrator] Google News daily slice: {nd} day(s) per google search feed "
                f"({params.google_slice_delay_s}s delay between day requests)."
            )
        for country in params.countries:
            feeds = feeds_by_country.get(country, [])
            if include_rss:
                print(f"[orchestrator] {country}: {len(feeds)} RSS source(s) configured")
            if mode in {"tavily", "hybrid"}:
                print(
                    f"[orchestrator] {country}: Tavily discovery enabled "
                    f"(max {getattr(params, 'tavily_discovery_max_results', 5)} per domain)"
                )

    def run(self, params: RunParams) -> int:
        mode = getattr(params, "discovery_mode", "rss")
        include_rss = mode in {"rss", "hybrid"} or (
            mode == "tavily" and getattr(params, "include_feeds_with_tavily", True)
        )
        feeds_by_country: dict[str, list[dict[str, str]]] = {}
        if include_rss:
            feeds_path = Path(params.feeds_path or "")
            if not feeds_path.exists():
                print(f"[orchestrator] Feeds file not found: {feeds_path}")
                return 1
            feeds_by_country = load_country_feeds(feeds_path)
        domain_registry = DomainAgentRegistry(active_domains=params.domains)
        start_d, end_d = _date_window(params)
        output_root = Path(params.output_dir).resolve()

        if params.dry_run:
            self.plan(params)
            print("[orchestrator] Dry-run: skipping HTTP fetch of RSS and articles.")
            return 0

        tavily_needed = bool(params.tavily_enrich) or getattr(params, "discovery_mode", "rss") in {"tavily", "hybrid"}
        if tavily_needed:
            try:
                from ml.web_data_mining.agentic.tavily_tools import is_tavily_configured

                if not is_tavily_configured():
                    print(
                        "[orchestrator] Tavily requested (enrich/discovery) but TAVILY_API_KEY missing or empty; "
                        "continuing without Tavily features."
                    )
                elif getattr(params, "tavily_use_langgraph", False):
                    try:
                        from ml.web_data_mining.agentic.mining_research_graph import (
                            mining_graph_llm_missing_reason,
                        )

                        msg = mining_graph_llm_missing_reason()
                        if msg:
                            print(f"[orchestrator] --tavily-langgraph: {msg}")
                    except Exception:
                        pass
            except Exception as exc:
                print(f"[orchestrator] Tavily availability check failed (RSS-only): {exc}")

        total_saved = 0
        seen_norm_urls: dict[str, str] = {}
        canonical_title: dict[str, tuple[str, str]] = {}
        canonical_body_hash: dict[str, str] = {}
        for country in params.countries:
            country_nodes_executed: list[str] = ["load_feeds", "discover_candidates"]
            feeds = feeds_by_country.get(country, [])
            mode = getattr(params, "discovery_mode", "rss")
            if include_rss and not feeds:
                print(f"[orchestrator] No feeds for country '{country}' — skip.")
                if mode == "rss":
                    continue

            candidates: list[tuple[RssItem, str, int]] = []
            debug_discovery = bool(getattr(params, "debug_discovery", False))
            rss_total_seen = 0
            rss_kept = 0
            tav_total_discovered = 0
            tav_drop_score = 0
            tav_drop_domain = 0
            tav_kept = 0
            if include_rss:
                for feed in feeds:
                    jobs = _feed_fetch_jobs(feed, params)
                    for j_idx, (fetch_url, fetch_name) in enumerate(jobs):
                        if j_idx > 0:
                            time.sleep(params.google_slice_delay_s)
                        try:
                            entries = fetch_feed_entries(fetch_url, fetch_name)
                        except Exception as exc:
                            print(f"[orchestrator] RSS fetch failed {fetch_name}: {exc}")
                            continue
                        for item in entries:
                            rss_total_seen += 1
                            if not item_in_date_window(item, start_d, end_d):
                                if debug_discovery:
                                    print(
                                        f"[orchestrator][discovery][rss][drop-date] "
                                        f"url={item.url} title={item.title[:120]!r}"
                                    )
                                continue
                            blob = f"{item.title} {item.summary}"
                            dom, score = domain_registry.best_domain(blob)
                            if score < self.min_domain_score:
                                if debug_discovery:
                                    print(
                                        f"[orchestrator][discovery][rss][drop-score] "
                                        f"score={score} < min={self.min_domain_score} "
                                        f"url={item.url} title={item.title[:120]!r}"
                                    )
                                continue
                            if dom not in params.domains:
                                if debug_discovery:
                                    print(
                                        f"[orchestrator][discovery][rss][drop-domain] "
                                        f"best_domain={dom!r} allowed={params.domains} "
                                        f"url={item.url} title={item.title[:120]!r}"
                                    )
                                continue
                            candidates.append((item, dom, score))
                            rss_kept += 1
                            if debug_discovery:
                                print(
                                    f"[orchestrator][discovery][rss][accept] "
                                    f"domain={dom!r} score={score} "
                                    f"url={item.url} title={item.title[:120]!r}"
                                )

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
                    tav_total_discovered = len(discovered)
                    tav_min_score = int(getattr(params, "tavily_discovery_min_domain_score", 1))
                    for item in discovered:
                        blob = f"{item.title} {item.summary}"
                        dom, score = domain_registry.best_domain(blob)
                        if score < tav_min_score:
                            tav_drop_score += 1
                            if debug_discovery:
                                print(
                                    f"[orchestrator][discovery][tavily][drop-score] "
                                    f"score={score} < min={tav_min_score} "
                                    f"url={item.url} title={item.title[:120]!r}"
                                )
                            continue
                        if dom not in params.domains:
                            tav_drop_domain += 1
                            if debug_discovery:
                                print(
                                    f"[orchestrator][discovery][tavily][drop-domain] "
                                    f"best_domain={dom!r} allowed={params.domains} "
                                    f"url={item.url} title={item.title[:120]!r}"
                                )
                            continue
                        candidates.append((item, dom, score))
                        tav_kept += 1
                        if debug_discovery:
                            print(
                                f"[orchestrator][discovery][tavily][accept] "
                                f"domain={dom!r} score={score} "
                                f"url={item.url} title={item.title[:120]!r}"
                            )
                except Exception as exc:
                    print(f"[orchestrator] Tavily discovery failed for {country}: {exc}")

            by_url: dict[str, tuple[RssItem, str, int]] = {}
            for item, dom, score in candidates:
                prev = by_url.get(item.url)
                if prev is None or score > prev[2]:
                    by_url[item.url] = (item, dom, score)
            country_nodes_executed.append("dedupe_rank")

            ordered = sorted(by_url.items(), key=lambda x: x[1][2], reverse=True)
            slice_urls = ordered[: params.max_urls_per_country]
            if include_rss:
                print(
                    f"[orchestrator] RSS discovery stats {country}: "
                    f"seen={rss_total_seen}, accepted={rss_kept}"
                )
            if mode in {"tavily", "hybrid"}:
                print(
                    f"[orchestrator] Tavily discovery stats {country}: "
                    f"discovered={tav_total_discovered}, "
                    f"dropped_score={tav_drop_score}, dropped_domain={tav_drop_domain}, accepted={tav_kept}"
                )
            print(
                f"[orchestrator] Candidate funnel {country}: "
                f"pre_dedupe={len(candidates)}, unique_urls={len(ordered)}, slice_urls={len(slice_urls)}"
            )

            for url, (item, dom, score) in slice_urls:
                nodes_executed = list(country_nodes_executed)
                if is_site_root_or_hub_url(item.url):
                    print(f"[orchestrator] Skip homepage/hub URL (RSS date vs live page mismatch risk): {item.url[:100]}")
                    continue
                body_source = "fetched_html"
                try:
                    nodes_executed.extend(["fetch_phase1", "resolve_article_url", "fetch_phase2", "clean_extract"])
                    page_title, body, fetch_url, phase1_fetch_url, extract_meta = fetch_and_extract(item)
                except Exception as exc:
                    print(f"[orchestrator] Article fetch failed {url[:80]}...: {exc}")
                    continue
                if is_google_consent_or_gate_page(page_title, body):
                    summary = (item.summary or "").strip()
                    if len(summary) >= self.min_rss_summary_chars:
                        body = summary
                        body_source = "rss_summary_google_gate"
                        page_title = item.title
                    elif params.tavily_enrich:
                        tavily_ok = False
                        try:
                            from ml.web_data_mining.agentic.tavily_tools import is_tavily_configured

                            tavily_ok = is_tavily_configured()
                        except Exception:
                            pass
                        if not tavily_ok:
                            print(
                                f"[orchestrator] Skip Google gate URL with short summary "
                                f"(set TAVILY_API_KEY for --tavily-enrich recovery): {url[:80]}..."
                            )
                            continue
                        # Keep thin/empty summary and try Tavily from title + RSS context below.
                        body = summary
                        body_source = "google_gate_tavily_pending"
                        page_title = item.title
                        print(
                            f"[orchestrator] Google gate + short RSS summary — attempting Tavily: {url[:80]}..."
                        )
                    else:
                        print(f"[orchestrator] Skip Google gate URL with short summary: {url[:80]}...")
                        continue
                elif len(body) < self.min_article_chars:
                    summary = (item.summary or "").strip()
                    if len(summary) >= self.min_rss_summary_chars and (
                        "news.google.com" in url or len(summary) >= self.min_article_chars
                    ):
                        body = summary
                        body_source = "rss_summary_fallback"
                        page_title = _prefer_item_title(page_title, item.title)
                    elif not params.tavily_enrich:
                        continue
                    # With --tavily-enrich, fall through: Tavily may recover thin bodies below.
                page_title = _prefer_item_title(page_title, item.title)
                final_title = page_title or item.title
                ho = _body_is_mostly_headline_only(final_title, body)
                thin = len(body.strip()) < self.min_article_chars

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

                # Guardrail: sometimes a PDF response gets decoded as text and looks like "%PDF-1.3 ...".
                # Never store that; it will poison downstream chunking.
                if body.lstrip().startswith("%PDF-"):
                    print(f"[orchestrator] Skip (raw PDF bytes decoded as text): {url[:80]}...")
                    continue
                if _looks_like_binary_text(body):
                    print(f"[orchestrator] Skip (binary bytes decoded as text): {url[:80]}...")
                    continue

                if params.tavily_enrich and (thin or ho):
                    nodes_executed.append("enrichment_decision")
                    try:
                        from ml.web_data_mining.agentic.enrichment import try_enrich_with_tavily

                        nodes_executed.append("tavily_enrich")
                        nb, nt, tag = try_enrich_with_tavily(
                            item=item,
                            body=body,
                            page_title=final_title,
                            fetch_url=fetch_url,
                            phase1_fetch_url=phase1_fetch_url,
                            min_chars=self.min_article_chars,
                            max_search_results=params.tavily_max_search_results,
                            try_extract=params.tavily_extract_first,
                            headline_only=ho,
                            country=country,
                            domain=dom,
                            use_langgraph=getattr(params, "tavily_use_langgraph", False),
                            graph_recursion_limit=getattr(params, "tavily_graph_recursion_limit", 28),
                        )
                        if tag:
                            body = nb
                            if nt:
                                page_title = nt
                            body_source = tag
                            if tag == "tavily_langgraph":
                                nodes_executed.append("optional_deep_research")
                            nodes_executed.append("merge_enrichment")
                            print(f"[orchestrator] Tavily enrich ok ({tag}): {url[:80]}...")
                    except Exception as exc:
                        print(f"[orchestrator] Tavily enrich failed {url[:60]}...: {exc}")
                else:
                    nodes_executed.append("enrichment_decision")

                page_title = _prefer_item_title(page_title, item.title)
                final_title = page_title or item.title

                if len(body.strip()) < self.min_article_chars:
                    print(
                        f"[orchestrator] Skip (body still short {_short_body_skip_hint(params)}): "
                        f"{url[:80]}..."
                    )
                    continue
                if _body_is_mostly_headline_only(final_title, body) and len(body.strip()) < self.min_article_chars:
                    print(f"[orchestrator] Skip headline-only / duplicate summary (no full article text): {url[:80]}...")
                    continue

                # Tavily Search = concatenated snippets, often multi-site; not a full article unless opted in.
                if body_source == "tavily_search":
                    if not getattr(params, "tavily_allow_search_snippet_save", False):
                        print(
                            f"[orchestrator] Skip tavily_search body (not a single full article; "
                            f"use --allow-tavily-search-snippets to opt in): {url[:80]}..."
                        )
                        continue
                    from ml.web_data_mining.agentic.tavily_search_quality import (
                        tavily_search_body_passes_quality,
                    )

                    ok, reason = tavily_search_body_passes_quality(
                        body=body,
                        country=country,
                        title=final_title,
                        max_distinct_domains=getattr(params, "tavily_search_max_distinct_domains", 2),
                        require_country_or_title_match=getattr(
                            params, "tavily_search_require_relevance", True
                        ),
                    )
                    if not ok:
                        print(f"[orchestrator] Skip tavily_search quality: {reason} — {url[:80]}...")
                        continue

                full_blob = f"{final_title}\n{body}"
                nodes_executed.extend(["extract_dates", "normalize_dates", "agrifood_gate", "domain_score"])
                agrifood_signals = agricultural_context_signals(full_blob)
                if not agrifood_signals:
                    print(f"[orchestrator] Skip non-agrifood article after full extraction: {url[:80]}...")
                    continue
                domain_scores = domain_registry.scores(full_blob)
                domain_labels = domain_registry.ranked_labels(full_blob)
                best_domain = domain_labels[0] if domain_labels else dom
                if best_domain not in params.domains:
                    print(f"[orchestrator] Skip best domain outside allow-list ({best_domain}): {url[:80]}...")
                    continue

                aid = article_id_from_url(url)
                ingested_at = utc_now_iso()
                extracted_at = (extract_meta.get("extracted_at") or utc_now_iso()) if extract_meta else utc_now_iso()
                html_published = (extract_meta.get("html_published_at") if extract_meta else None) or None
                article_updated_at = (extract_meta.get("article_updated_at") if extract_meta else None) or None
                published = pick_published_at(
                    rss_date=item.published,
                    html_published_iso=html_published,
                    tavily_published_iso=None,
                    inferred_iso=ingested_at,
                )

                norm_url = normalize_url_for_dedupe(fetch_url or url)
                host = urlparse(fetch_url or url).netloc.lower()
                this_dedupe_id = dedupe_id(final_title, host)
                this_cluster_id = cluster_id(final_title, country, dom, published.published_at)
                this_body_hash = content_hash(body)
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
                    "html_published_raw": extract_meta.get("html_published_raw") if extract_meta else None,
                    "article_updated_at": article_updated_at,
                    "html_updated_raw": extract_meta.get("html_updated_raw") if extract_meta else None,
                    "ingested_at": ingested_at,
                    "extracted_at": extracted_at,
                    "feed_name": item.feed_name,
                    "source": "rss",
                    "body_source": body_source,
                    "phase1_fetch_url": phase1_fetch_url,
                    "article_fetch_url": fetch_url,
                    "enriched": body_source.startswith("tavily_"),
                    "enrichment_used": body_source if body_source.startswith("tavily_") else None,
                    "dedupe_id": this_dedupe_id,
                    "cluster_id": this_cluster_id,
                    "is_duplicate": is_dup,
                    "duplicate_of": duplicate_of,
                    "agrifood_signal": True,
                    "agrifood_signals": agrifood_signals[:25],
                    "domain_scores": domain_scores,
                    "domain_labels": domain_labels,
                    "best_domain": best_domain,
                    "pipeline_nodes_executed": nodes_executed + ["store_article"],
                }
                if fetch_url != url:
                    meta["fetch_url"] = fetch_url
                try:
                    path = write_news_txt(output_root, country, meta, body)
                    total_saved += 1
                    print(f"[orchestrator] saved {path}")
                except Exception:
                    print(f"[orchestrator] storage failed for {url}:\n{traceback.format_exc()}")

        print(f"[orchestrator] Done. Saved {total_saved} article file(s) under {output_root}")
        return 0

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from ml.web_data_mining.google_news_slice import count_slice_days, slice_range_from_params


DEFAULT_DOMAINS = [
    "agriculture",
    "economics",
    "international trade (exports & imports)",
    "environmental & climate",
    "land use & soil health",
    "investment readiness & enterprise",
    "technology & innovation",
    "market access & infrastructure",
    "production & yield",
    "policy & institutional",
    "gender, youth & inclusion",
    "nutrition & food security",
    "food systems & value chain",
    "humanitarian & agricultural emergency",
]


def normalize_country_name(raw: str) -> str:
    cleaned = " ".join(raw.strip().split())
    if not cleaned:
        return ""
    aliases = {
        "sierraLeone".lower(): "Sierra Leone",
        "sierraleone".lower(): "Sierra Leone",
        "cotedivoire".lower(): "Cote d'Ivoire",
        "ivorycoast".lower(): "Cote d'Ivoire",
    }
    key = cleaned.replace(" ", "").replace("-", "").lower()
    if key in aliases:
        return aliases[key]
    return cleaned.title()


def parse_countries(raw: str | None) -> list[str]:
    if not raw:
        return []
    items = [normalize_country_name(x) for x in raw.split(",")]
    out: list[str] = []
    for item in items:
        if item and item not in out:
            out.append(item)
    return out


def parse_domains(raw: str | None) -> list[str]:
    if not raw:
        return []
    items = [" ".join(x.strip().split()) for x in raw.split(",")]
    out: list[str] = []
    for item in items:
        if item and item not in out:
            out.append(item)
    return out


def parse_iso_date(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


@dataclass
class RunParams:
    countries: list[str]
    domains: list[str]
    start_year: int | None = None
    end_year: int | None = None
    start_date: date | None = None
    end_date: date | None = None
    dry_run: bool = False
    batch_size: int = 100
    max_urls_per_country: int = 500
    # Directory root for saved .txt articles (YAML front matter + body).
    output_dir: str = "data/local/web_news_rss"
    # Path to JSON mapping country -> list of {name, url} RSS feeds.
    feeds_path: str | None = None
    # If True, each news.google.com/rss/search feed is fetched once per calendar day in the
    # selected range (after:/before: in q). Heavy; use small date windows or --allow-large-google-slice.
    google_news_daily_slice: bool = False
    google_slice_delay_s: float = 1.25
    allow_large_google_slice: bool = False
    # Optional Tavily (langchain-tavily): enrich thin RSS/fetch results without replacing RSS.
    tavily_enrich: bool = False
    tavily_max_search_results: int = 3
    tavily_extract_first: bool = True
    # LangGraph + LLM iterative Tavily research (see deep_research_from_scratch); requires graph extras + OPENAI_API_KEY.
    tavily_use_langgraph: bool = False
    tavily_graph_recursion_limit: int = 28
    # Minimum saved article body length; below this (after fallbacks + optional Tavily) items are skipped.
    min_article_chars: int = 200
    # Minimum RSS summary length to use as body for Google gate / thin-fetch fallback without Tavily.
    min_rss_summary_chars: int = 80
    # Tavily Search returns multi-snippet blobs (not one full article). Default: do not save them.
    tavily_allow_search_snippet_save: bool = False
    tavily_search_max_distinct_domains: int = 2
    tavily_search_require_relevance: bool = True
    # Discovery source for candidate URLs before fetch/extract.
    # rss = feeds only, tavily = Tavily search only, hybrid = union of both.
    discovery_mode: str = "rss"
    # When discovery_mode=tavily, still pull candidates from feeds.json for max recall.
    include_feeds_with_tavily: bool = True
    tavily_discovery_max_results: int = 5
    # Minimum domain score used at the Tavily discovery gate before fetch/extract.
    tavily_discovery_min_domain_score: int = 1
    # Verbose per-URL logs for discovery accept/reject reasons.
    debug_discovery: bool = False
    # If true, execute the top-level pipeline through a compiled LangGraph state graph.
    use_pipeline_langgraph: bool = False
    # Verbose per-node/per-article logs for pipeline_graph mode.
    debug_pipeline_graph: bool = False

    def validate(self) -> None:
        if not self.countries:
            raise ValueError("At least one country is required.")
        if not self.domains:
            raise ValueError("At least one domain is required.")

        if self.start_date or self.end_date:
            if not (self.start_date and self.end_date):
                raise ValueError("Both start_date and end_date are required when using date range.")
            if self.start_date > self.end_date:
                raise ValueError("start_date must be <= end_date.")
        else:
            if self.start_year is None or self.end_year is None:
                raise ValueError("Provide either (start_year and end_year) or (start_date and end_date).")
            if self.start_year > self.end_year:
                raise ValueError("start_year must be <= end_year.")
            if self.start_year < 1900 or self.end_year > 2100:
                raise ValueError("Year range out of bounds (valid: 1900-2100).")

        if self.google_slice_delay_s <= 0:
            raise ValueError("google_slice_delay_s must be > 0.")

        if self.tavily_max_search_results < 1 or self.tavily_max_search_results > 20:
            raise ValueError("tavily_max_search_results must be between 1 and 20.")

        if self.tavily_use_langgraph and not self.tavily_enrich:
            raise ValueError("tavily_use_langgraph requires tavily_enrich=True (e.g. --tavily-enrich).")

        if self.tavily_graph_recursion_limit < 8 or self.tavily_graph_recursion_limit > 100:
            raise ValueError("tavily_graph_recursion_limit must be between 8 and 100.")

        if self.min_article_chars < 50 or self.min_article_chars > 4000:
            raise ValueError("min_article_chars must be between 50 and 4000.")
        if self.min_rss_summary_chars < 0 or self.min_rss_summary_chars > 2000:
            raise ValueError("min_rss_summary_chars must be between 0 and 2000.")

        if self.tavily_search_max_distinct_domains < 1 or self.tavily_search_max_distinct_domains > 20:
            raise ValueError("tavily_search_max_distinct_domains must be between 1 and 20.")
        if self.discovery_mode not in {"rss", "tavily", "hybrid"}:
            raise ValueError("discovery_mode must be one of: rss, tavily, hybrid.")
        if self.tavily_discovery_max_results < 1 or self.tavily_discovery_max_results > 20:
            raise ValueError("tavily_discovery_max_results must be between 1 and 20.")
        if self.tavily_discovery_min_domain_score < 0 or self.tavily_discovery_min_domain_score > 10:
            raise ValueError("tavily_discovery_min_domain_score must be between 0 and 10.")
        if not isinstance(self.use_pipeline_langgraph, bool):
            raise ValueError("use_pipeline_langgraph must be boolean.")
        if not isinstance(self.debug_pipeline_graph, bool):
            raise ValueError("debug_pipeline_graph must be boolean.")
        if self.google_news_daily_slice:
            try:
                sr, er = slice_range_from_params(
                    self.start_date,
                    self.end_date,
                    self.start_year,
                    self.end_year,
                )
            except ValueError as exc:
                raise ValueError(f"Google News daily slice: {exc}") from exc
            n = count_slice_days(sr, er)
            soft_max = 93
            if n > soft_max and not self.allow_large_google_slice:
                raise ValueError(
                    f"Google News daily slice covers {n} days (~{n} extra RSS GETs per Google feed). "
                    "Use a shorter --start-date/--end-date (or year) window, or pass "
                    "--allow-large-google-slice (slow; may be rate-limited)."
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "countries": self.countries,
            "domains": self.domains,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "dry_run": self.dry_run,
            "batch_size": self.batch_size,
            "max_urls_per_country": self.max_urls_per_country,
            "output_dir": self.output_dir,
            "feeds_path": self.feeds_path,
            "google_news_daily_slice": self.google_news_daily_slice,
            "google_slice_delay_s": self.google_slice_delay_s,
            "allow_large_google_slice": self.allow_large_google_slice,
            "tavily_enrich": self.tavily_enrich,
            "tavily_max_search_results": self.tavily_max_search_results,
            "tavily_extract_first": self.tavily_extract_first,
            "tavily_use_langgraph": self.tavily_use_langgraph,
            "tavily_graph_recursion_limit": self.tavily_graph_recursion_limit,
            "min_article_chars": self.min_article_chars,
            "min_rss_summary_chars": self.min_rss_summary_chars,
            "tavily_allow_search_snippet_save": self.tavily_allow_search_snippet_save,
            "tavily_search_max_distinct_domains": self.tavily_search_max_distinct_domains,
            "tavily_search_require_relevance": self.tavily_search_require_relevance,
            "discovery_mode": self.discovery_mode,
            "include_feeds_with_tavily": self.include_feeds_with_tavily,
            "tavily_discovery_max_results": self.tavily_discovery_max_results,
            "tavily_discovery_min_domain_score": self.tavily_discovery_min_domain_score,
            "debug_discovery": self.debug_discovery,
            "use_pipeline_langgraph": self.use_pipeline_langgraph,
            "debug_pipeline_graph": self.debug_pipeline_graph,
        }


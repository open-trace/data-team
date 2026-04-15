from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ml.web_data_mining.schemas import DEFAULT_DOMAINS, RunParams, parse_countries, parse_domains, parse_iso_date

_PACKAGE_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _PACKAGE_DIR / "config"


def _default_feeds_path() -> Path:
    """Prefer curated feeds.json when present; otherwise example file."""
    curated = _CONFIG_DIR / "feeds.json"
    if curated.exists():
        return curated
    return _CONFIG_DIR / "feeds.example.json"


def load_config_file(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    suffix = path.suffix.lower()
    raw_text = path.read_text(encoding="utf-8")
    if suffix == ".json":
        return json.loads(raw_text)
    if suffix in {".yml", ".yaml"}:
        try:
            import yaml  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError("YAML config requires pyyaml. Install with: pip install pyyaml") from exc
        parsed = yaml.safe_load(raw_text) or {}
        if not isinstance(parsed, dict):
            raise ValueError("YAML config must contain a top-level mapping/object.")
        return parsed
    raise ValueError("Unsupported config format. Use .json, .yml, or .yaml")


def _parse_countries_from_any(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return parse_countries(",".join(str(x) for x in value))
    return parse_countries(str(value))


def _parse_domains_from_any(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: list[str] = []
        for x in value:
            item = " ".join(str(x).strip().split())
            if item and item not in out:
                out.append(item)
        return out
    return parse_domains(str(value))


def resolve_run_params(args: Any, config_data: dict[str, Any]) -> RunParams:
    """
    Resolve params with precedence: CLI > config > defaults.
    """
    countries = parse_countries(args.countries) or _parse_countries_from_any(config_data.get("countries"))
    domains = parse_domains(args.domains) or _parse_domains_from_any(config_data.get("domains")) or list(DEFAULT_DOMAINS)

    # Date values override year values when provided.
    start_date_raw = args.start_date if args.start_date is not None else config_data.get("start_date")
    end_date_raw = args.end_date if args.end_date is not None else config_data.get("end_date")
    start_date = parse_iso_date(start_date_raw) if start_date_raw else None
    end_date = parse_iso_date(end_date_raw) if end_date_raw else None

    current_year_default = 2005
    start_year = args.start_year if args.start_year is not None else config_data.get("start_year", current_year_default)
    end_year = args.end_year if args.end_year is not None else config_data.get("end_year")

    if end_year is None:
        from datetime import datetime

        end_year = datetime.utcnow().year

    output_dir = getattr(args, "output_dir", None) or config_data.get("output_dir") or "data/local/web_news_rss"
    feeds_path_raw = getattr(args, "feeds", None) or config_data.get("feeds_path") or config_data.get("feeds")
    if feeds_path_raw:
        feeds_path = str(Path(feeds_path_raw).expanduser() if isinstance(feeds_path_raw, Path) else Path(str(feeds_path_raw)).expanduser())
    else:
        feeds_path = str(_default_feeds_path())

    google_news_daily_slice = bool(config_data.get("google_news_daily_slice", False))
    if getattr(args, "google_news_daily_slice", False):
        google_news_daily_slice = True

    allow_large_google_slice = bool(config_data.get("allow_large_google_slice", False))
    if getattr(args, "allow_large_google_slice", False):
        allow_large_google_slice = True

    slice_delay_raw = getattr(args, "google_slice_delay", None)
    if slice_delay_raw is not None:
        google_slice_delay_s = float(slice_delay_raw)
    else:
        google_slice_delay_s = float(config_data.get("google_slice_delay_s", 1.25))

    tavily_enrich = bool(config_data.get("tavily_enrich", False))
    if getattr(args, "tavily_enrich", False):
        tavily_enrich = True

    tavily_max_raw = getattr(args, "tavily_max_search_results", None)
    if tavily_max_raw is not None:
        tavily_max_search_results = int(tavily_max_raw)
    else:
        tavily_max_search_results = int(config_data.get("tavily_max_search_results", 3))

    tavily_extract_first = bool(config_data.get("tavily_extract_first", True))
    if getattr(args, "tavily_no_extract_first", False):
        tavily_extract_first = False

    tavily_use_langgraph = bool(config_data.get("tavily_use_langgraph", False))
    if getattr(args, "tavily_langgraph", False):
        tavily_use_langgraph = True

    tgr_raw = getattr(args, "tavily_graph_recursion_limit", None)
    if tgr_raw is not None:
        tavily_graph_recursion_limit = int(tgr_raw)
    else:
        tavily_graph_recursion_limit = int(config_data.get("tavily_graph_recursion_limit", 28))

    mac_raw = getattr(args, "min_article_chars", None)
    if mac_raw is not None:
        min_article_chars = int(mac_raw)
    else:
        min_article_chars = int(config_data.get("min_article_chars", 200))

    mrss_raw = getattr(args, "min_rss_summary_chars", None)
    if mrss_raw is not None:
        min_rss_summary_chars = int(mrss_raw)
    else:
        min_rss_summary_chars = int(config_data.get("min_rss_summary_chars", 80))

    tavily_allow_search_snippet_save = bool(config_data.get("tavily_allow_search_snippet_save", False))
    if getattr(args, "allow_tavily_search_snippets", False):
        tavily_allow_search_snippet_save = True

    tsmd_raw = getattr(args, "tavily_search_max_distinct_domains", None)
    if tsmd_raw is not None:
        tavily_search_max_distinct_domains = int(tsmd_raw)
    else:
        tavily_search_max_distinct_domains = int(config_data.get("tavily_search_max_distinct_domains", 2))

    tavily_search_require_relevance = bool(config_data.get("tavily_search_require_relevance", True))
    if getattr(args, "no_tavily_search_relevance", False):
        tavily_search_require_relevance = False

    discovery_mode = str(getattr(args, "discovery_mode", None) or config_data.get("discovery_mode") or "rss").strip().lower()
    include_feeds_with_tavily = bool(config_data.get("include_feeds_with_tavily", True))
    if getattr(args, "no_include_feeds_with_tavily", False):
        include_feeds_with_tavily = False
    tavily_discovery_raw = getattr(args, "tavily_discovery_max_results", None)
    if tavily_discovery_raw is not None:
        tavily_discovery_max_results = int(tavily_discovery_raw)
    else:
        tavily_discovery_max_results = int(config_data.get("tavily_discovery_max_results", 5))
    tdm_raw = getattr(args, "tavily_discovery_min_domain_score", None)
    if tdm_raw is not None:
        tavily_discovery_min_domain_score = int(tdm_raw)
    else:
        tavily_discovery_min_domain_score = int(config_data.get("tavily_discovery_min_domain_score", 1))
    debug_discovery = bool(config_data.get("debug_discovery", False))
    if getattr(args, "debug_discovery", False):
        debug_discovery = True
    use_pipeline_langgraph = bool(config_data.get("use_pipeline_langgraph", False))
    if getattr(args, "pipeline_langgraph", False):
        use_pipeline_langgraph = True
    debug_pipeline_graph = bool(config_data.get("debug_pipeline_graph", False))
    if getattr(args, "debug_pipeline_graph", False):
        debug_pipeline_graph = True

    params = RunParams(
        countries=countries,
        domains=domains,
        start_year=None if start_date else int(start_year),
        end_year=None if end_date else int(end_year),
        start_date=start_date,
        end_date=end_date,
        dry_run=bool(args.dry_run),
        batch_size=int(args.batch_size),
        max_urls_per_country=int(args.max_urls_per_country),
        output_dir=str(output_dir),
        feeds_path=feeds_path,
        google_news_daily_slice=google_news_daily_slice,
        google_slice_delay_s=google_slice_delay_s,
        allow_large_google_slice=allow_large_google_slice,
        tavily_enrich=tavily_enrich,
        tavily_max_search_results=tavily_max_search_results,
        tavily_extract_first=tavily_extract_first,
        tavily_use_langgraph=tavily_use_langgraph,
        tavily_graph_recursion_limit=tavily_graph_recursion_limit,
        min_article_chars=min_article_chars,
        min_rss_summary_chars=min_rss_summary_chars,
        tavily_allow_search_snippet_save=tavily_allow_search_snippet_save,
        tavily_search_max_distinct_domains=tavily_search_max_distinct_domains,
        tavily_search_require_relevance=tavily_search_require_relevance,
        discovery_mode=discovery_mode,
        include_feeds_with_tavily=include_feeds_with_tavily,
        tavily_discovery_max_results=tavily_discovery_max_results,
        tavily_discovery_min_domain_score=tavily_discovery_min_domain_score,
        debug_discovery=debug_discovery,
        use_pipeline_langgraph=use_pipeline_langgraph,
        debug_pipeline_graph=debug_pipeline_graph,
    )
    params.validate()
    return params


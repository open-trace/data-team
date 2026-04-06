from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ml.web_data_mining.config import load_config_file, resolve_run_params
from ml.web_data_mining.pipeline import run_pipeline


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run web data mining pipeline with dynamic countries and period."
    )
    parser.add_argument(
        "--countries",
        type=str,
        help="Comma-separated countries (e.g. Nigeria,Rwanda,Ghana).",
    )
    parser.add_argument(
        "--domains",
        type=str,
        help="Comma-separated domains. If omitted, defaults to all configured domains.",
    )
    parser.add_argument("--start-year", type=int, help="Inclusive start year.")
    parser.add_argument("--end-year", type=int, help="Inclusive end year.")
    parser.add_argument("--start-date", type=str, help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", type=str, help="Inclusive end date (YYYY-MM-DD).")
    parser.add_argument("--config", type=Path, help="Optional config path (.json/.yml/.yaml).")
    parser.add_argument("--batch-size", type=int, default=100, help="Pipeline batch size.")
    parser.add_argument(
        "--max-urls-per-country",
        type=int,
        default=500,
        help="Max URLs to process per country.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved params and planned query windows without fetching.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Root directory for saved .txt articles (YAML front matter + body).",
    )
    parser.add_argument(
        "--feeds",
        type=Path,
        default=None,
        help="JSON: country -> [{name, url}] RSS feeds. Default: feeds.json or feeds.example.json",
    )
    parser.add_argument(
        "--google-news-daily-slice",
        action="store_true",
        help=(
            "For news.google.com/rss/search feeds only: fetch once per calendar day in the run's date/year "
            "window (adds after:/before: to q). Outlet RSS is unchanged. Slow for long ranges; use "
            "--allow-large-google-slice beyond ~93 days."
        ),
    )
    parser.add_argument(
        "--allow-large-google-slice",
        action="store_true",
        help="Allow Google News daily slice over more than ~93 days (many HTTP requests; rate limits likely).",
    )
    parser.add_argument(
        "--google-slice-delay",
        type=float,
        default=None,
        help="Seconds to sleep between daily Google News RSS requests (default 1.25).",
    )
    parser.add_argument(
        "--tavily-enrich",
        action="store_true",
        help=(
            "After normal RSS fetch/extract, try Tavily Extract + Tavily Search when body is still thin "
            "(requires TAVILY_API_KEY and: pip install -r ml/web_data_mining/requirements-agent.txt)."
        ),
    )
    parser.add_argument(
        "--tavily-max-search-results",
        type=int,
        default=None,
        help="Max Tavily search hits when --tavily-enrich is on (default 3, max 20).",
    )
    parser.add_argument(
        "--tavily-no-extract-first",
        action="store_true",
        help="Skip Tavily Extract on publisher URLs; only use Tavily Search.",
    )
    parser.add_argument(
        "--tavily-langgraph",
        action="store_true",
        help=(
            "Use LangGraph deep-research loop (LLM + Tavily tools + compress) for thin items. "
            "Requires --tavily-enrich, pip install -r ml/web_data_mining/requirements-agent-graph.txt, "
            "OPENAI_API_KEY, and TAVILY_API_KEY."
        ),
    )
    parser.add_argument(
        "--tavily-graph-recursion-limit",
        type=int,
        default=None,
        help="Max LangGraph steps for --tavily-langgraph (default 28, max 100).",
    )
    parser.add_argument(
        "--min-article-chars",
        type=int,
        default=None,
        help="Minimum body length to save an article (default 200, range 50–4000).",
    )
    parser.add_argument(
        "--min-rss-summary-chars",
        type=int,
        default=None,
        help=(
            "Minimum RSS summary length to use as body for Google gate / thin HTML without Tavily "
            "(default 80). With --tavily-enrich, shorter summaries still get a Tavily recovery attempt."
        ),
    )
    parser.add_argument(
        "--allow-tavily-search-snippets",
        action="store_true",
        help=(
            "Allow saving bodies from Tavily Search (multi-snippet, not one full article). "
            "Default: skip those saves; prefer tavily_extract / LangGraph."
        ),
    )
    parser.add_argument(
        "--tavily-search-max-distinct-domains",
        type=int,
        default=None,
        help="With --allow-tavily-search-snippets: max distinct URL domains in text (default 2).",
    )
    parser.add_argument(
        "--no-tavily-search-relevance",
        action="store_true",
        help="With --allow-tavily-search-snippets: skip country/title relevance check.",
    )
    parser.add_argument(
        "--discovery-mode",
        type=str,
        default=None,
        choices=["rss", "tavily", "hybrid"],
        help=(
            "Candidate URL discovery source before fetch/extract: rss (feeds only), "
            "tavily (Tavily search only), hybrid (union). Default rss."
        ),
    )
    parser.add_argument(
        "--no-include-feeds-with-tavily",
        action="store_true",
        help="When discovery-mode=tavily, disable feed-based candidates (default is to include feeds too).",
    )
    parser.add_argument(
        "--tavily-discovery-max-results",
        type=int,
        default=None,
        help="Per-domain Tavily discovery result cap for discovery-mode tavily/hybrid (default 5).",
    )
    parser.add_argument(
        "--tavily-discovery-min-domain-score",
        type=int,
        default=None,
        help=(
            "Minimum domain score for Tavily-discovered candidates before fetch/extract "
            "(default 1; set 0 to keep more candidates)."
        ),
    )
    parser.add_argument(
        "--debug-discovery",
        action="store_true",
        help="Verbose per-URL discovery logs (accepted/rejected and reason).",
    )
    parser.add_argument(
        "--pipeline-langgraph",
        action="store_true",
        help=(
            "Execute the top-level deterministic pipeline through a compiled LangGraph state graph "
            "(node-sequenced orchestration; still RSS-first)."
        ),
    )
    parser.add_argument(
        "--debug-pipeline-graph",
        action="store_true",
        help="Verbose per-node/per-article diagnostics for --pipeline-langgraph mode.",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    try:
        config_data = load_config_file(args.config)
        params = resolve_run_params(args, config_data)
    except Exception as exc:
        print(f"Parameter/config error: {exc}", file=sys.stderr)
        return 2

    print("Resolved runtime params:")
    print(json.dumps(params.to_dict(), indent=2))
    return run_pipeline(params)


if __name__ == "__main__":
    raise SystemExit(main())

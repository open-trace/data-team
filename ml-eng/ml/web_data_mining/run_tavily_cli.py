"""
Quick CLI: run Tavily Search (news) for a query — no LLM required.

Requires: pip install -r ml/web_data_mining/requirements-agent.txt
Env: TAVILY_API_KEY
"""
from __future__ import annotations

import argparse
import json
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Tavily news search (standalone smoke / research).")
    parser.add_argument("--query", "-q", type=str, required=True, help="Search query.")
    parser.add_argument("--max-results", type=int, default=5, help="Max results (default 5).")
    parser.add_argument("--json", action="store_true", help="Print raw JSON-ish structure.")
    args = parser.parse_args()

    from ml.web_data_mining.agentic.tavily_tools import tavily_search_news

    text, results, err = tavily_search_news(args.query, max_results=args.max_results)
    if err:
        print(err, file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps({"snippet": text, "results": results}, indent=2, default=str))
    else:
        print(text or "(no text)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

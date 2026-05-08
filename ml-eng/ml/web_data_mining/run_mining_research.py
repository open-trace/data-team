"""
Run the LangGraph mining research agent on a freeform question (no RSS).

Requires: requirements-agent-graph.txt + TAVILY_API_KEY + OPENAI_API_KEY
"""
from __future__ import annotations

import argparse
import json
import sys


def main() -> int:
    p = argparse.ArgumentParser(
        description="LangGraph + Tavily deep-research style run (OpenTrace mining agent)."
    )
    p.add_argument("--question", "-q", type=str, required=True, help="Research question or topic.")
    p.add_argument(
        "--recursion-limit",
        type=int,
        default=28,
        help="LangGraph recursion limit (default 28).",
    )
    p.add_argument("--json", action="store_true", help="Print JSON with body + error fields.")
    args = p.parse_args()

    try:
        from ml.web_data_mining.agentic.mining_research_graph import run_mining_research_freeform
    except ImportError as exc:
        print(
            "Missing dependencies. Install: pip install -r ml/web_data_mining/requirements-agent-graph.txt",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
        return 2

    res = run_mining_research_freeform(args.question, recursion_limit=args.recursion_limit)
    if args.json:
        print(json.dumps({"ok": res.ok, "body": res.body, "error": res.error}, indent=2))
        return 0 if res.ok else 3
    if res.error and not res.body:
        print(res.error, file=sys.stderr)
        return 3
    print(res.body)
    return 0 if res.ok else 3


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Reusable ingest runner — thin wrapper around ``python -m ingestion run``.

Usage (from data-eng/):

  python ingestion/scripts/run_source.py hdx_wfp_prices --dry-run --limit 500
  python ingestion/scripts/run_source.py hdx_wfp_prices --limit 1000

Or via make:

  make ingest-dry SOURCE=hdx_wfp_prices
  make ingest SOURCE=hdx_wfp_prices
"""

from __future__ import annotations

import subprocess
import sys


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in {"-h", "--help"}:
        print(__doc__.strip(), file=sys.stderr)
        return 0 if args else 2
    cmd = [sys.executable, "-m", "ingestion", "run", *args]
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Sync data from all bronze tables (listed in bronze_tables.txt) into the local database.
Calls bq_partition_to_local.py for each table. Run after creating tables with
bq_schema_to_local_pg.py. Uses BQ_PARTITION_LIMIT and BQ_PARTITION_FILTER from env.

Usage:
  Set BQ_PROJECT, BQ_DATASET, LOCAL_DB_URL (or LOCAL_DB_PATH). Optionally BQ_PARTITION_LIMIT (default 10000).
  python sync_all_bronze_tables.py [--limit N]
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = Path(__file__).resolve().parent
SYNC_SCRIPT = SCRIPTS_DIR / "bq_partition_to_local.py"


def load_bronze_table_list() -> list[str]:
    path = SCRIPTS_DIR / "bronze_tables.txt"
    if not path.exists():
        return []
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Sync all bronze tables from BigQuery to local DB")
    parser.add_argument("--limit", type=int, default=None, help="Override BQ_PARTITION_LIMIT")
    args = parser.parse_args()

    if not SYNC_SCRIPT.exists():
        print(f"Sync script not found: {SYNC_SCRIPT}", file=sys.stderr)
        sys.exit(1)

    table_ids = load_bronze_table_list()
    if not table_ids:
        print("No tables in bronze_tables.txt", file=sys.stderr)
        sys.exit(1)

    env = os.environ.copy()
    if args.limit is not None:
        env["BQ_PARTITION_LIMIT"] = str(args.limit)

    for table_id in table_ids:
        if not table_id:
            continue
        cmd = [
            sys.executable,
            str(SYNC_SCRIPT),
            "--table", table_id,
            "--target-table", table_id,
        ]
        print(f"Syncing {table_id} ...")
        r = subprocess.run(cmd, env=env, cwd=str(REPO_ROOT))
        if r.returncode != 0:
            print(f"Warning: {table_id} failed (exit {r.returncode})", file=sys.stderr)

    print("Done.")


if __name__ == "__main__":
    main()

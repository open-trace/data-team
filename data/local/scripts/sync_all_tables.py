#!/usr/bin/env python3
"""
Sync data from BigQuery into the local database for a given layer (bronze, silver, or gold).
Calls bq_partition_to_local.py for each table in {layer}_tables.txt. Run after creating
tables with bq_schema_to_local_pg.py.

Usage:
  Set BQ_PROJECT, BQ_DATASET_BRONZE / BQ_DATASET_SILVER / BQ_DATASET_GOLD, LOCAL_DB_URL (or LOCAL_DB_PATH).
  python sync_all_tables.py --dataset bronze [--limit N]
  python sync_all_tables.py --dataset silver
  python sync_all_tables.py --dataset gold
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = Path(__file__).resolve().parent
SYNC_SCRIPT = SCRIPTS_DIR / "bq_partition_to_local.py"

LAYERS = ("bronze", "silver", "gold")
TABLE_FILES = {"bronze": "bronze_tables.txt", "silver": "silver_tables.txt", "gold": "gold_tables.txt"}
DATASET_ENV = {"bronze": "BQ_DATASET_BRONZE", "silver": "BQ_DATASET_SILVER", "gold": "BQ_DATASET_GOLD"}


def load_table_list(dataset: str) -> list[str]:
    path = SCRIPTS_DIR / TABLE_FILES[dataset]
    if not path.exists():
        return []
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Sync BigQuery tables to local DB for a layer (bronze/silver/gold)")
    parser.add_argument("--dataset", choices=LAYERS, required=True, help="Layer to sync")
    parser.add_argument("--limit", type=int, default=None, help="Override BQ_PARTITION_LIMIT")
    args = parser.parse_args()

    if not SYNC_SCRIPT.exists():
        print(f"Sync script not found: {SYNC_SCRIPT}", file=sys.stderr)
        sys.exit(1)

    table_ids = load_table_list(args.dataset)
    if not table_ids:
        print(f"No tables in {TABLE_FILES[args.dataset]}", file=sys.stderr)
        sys.exit(0)

    env = os.environ.copy()
    env["BQ_DATASET"] = os.environ.get(DATASET_ENV[args.dataset], args.dataset)
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
        print(f"Syncing {args.dataset}/{table_id} ...")
        r = subprocess.run(cmd, env=env, cwd=str(REPO_ROOT))
        if r.returncode != 0:
            print(f"Warning: {table_id} failed (exit {r.returncode})", file=sys.stderr)

    print("Done.")


if __name__ == "__main__":
    main()

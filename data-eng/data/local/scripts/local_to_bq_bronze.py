#!/usr/bin/env python3
"""
Load all bronze tables from the local database into BigQuery bronze dataset.

Reads table names from data/local/scripts/bronze_tables.txt. For each table,
reads from the local DB (PostgreSQL or SQLite via engine_connector) and loads
into BQ_PROJECT.BQ_DATASET_BRONZE.<table> with if_exists="replace".

Usage (from repo root):
  python data/local/scripts/local_to_bq_bronze.py

  python data/local/scripts/local_to_bq_bronze.py --dry-run   # list tables only
  python data/local/scripts/local_to_bq_bronze.py --tables fews_net_food_security_master yield_raw_data  # only these

Requires: pandas, pandas-gbq, sqlalchemy (+ psycopg2 if PostgreSQL). Set BQ_PROJECT,
BQ_DATASET_BRONZE, LOCAL_DB_URL or LOCAL_DB_PATH in data/local/.env.
Auth: GOOGLE_APPLICATION_CREDENTIALS or gcloud auth.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = Path(__file__).resolve().parent
BRONZE_TABLES_FILE = SCRIPTS_DIR / "bronze_tables.txt"

# Valid BQ table ID: letters, numbers, underscore only (skip e.g. "new dataset")
BQ_TABLE_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")


def _load_dotenv() -> None:
    env_file = REPO_ROOT / "data" / "local" / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().replace("export ", "", 1).strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


def load_bronze_table_list() -> list[str]:
    """Return table names from bronze_tables.txt, skipping comments and invalid names."""
    if not BRONZE_TABLES_FILE.exists():
        return []
    tables = []
    for line in BRONZE_TABLES_FILE.read_text().splitlines():
        name = line.strip()
        if not name or name.startswith("#"):
            continue
        if BQ_TABLE_PATTERN.match(name):
            tables.append(name)
    return tables


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Load local DB tables into BigQuery bronze dataset")
    parser.add_argument("--dry-run", action="store_true", help="Only list tables that would be loaded")
    parser.add_argument("--tables", nargs="*", default=None, help="Restrict to these table names (default: all in bronze_tables.txt)")
    args = parser.parse_args()

    _load_dotenv()
    project = os.environ.get("BQ_PROJECT", "").strip()
    dataset = os.environ.get("BQ_DATASET_BRONZE", "bronze").strip()
    if not project:
        print("Set BQ_PROJECT (e.g. in data/local/.env)", file=sys.stderr)
        sys.exit(1)

    if args.tables is not None:
        table_list = [t for t in args.tables if BQ_TABLE_PATTERN.match(t)]
    else:
        table_list = load_bronze_table_list()

    if not table_list:
        print("No tables to load. Add names to data/local/scripts/bronze_tables.txt or use --tables.", file=sys.stderr)
        sys.exit(1)

    print(f"Target: {project}.{dataset}")
    print(f"Tables: {len(table_list)}")
    for t in table_list:
        print(f"  - {t}")

    if args.dry_run:
        print("Dry run: no load performed.")
        return

    try:
        import pandas as pd
    except ImportError:
        print("Install: pip install pandas", file=sys.stderr)
        sys.exit(1)
    try:
        import pandas_gbq
    except ImportError:
        print("Install: pip install pandas-gbq", file=sys.stderr)
        sys.exit(1)

    sys.path.insert(0, str(SCRIPTS_DIR))
    from engine_connector import get_engine  # noqa: E402

    engine = get_engine()
    ok = 0
    skip = 0
    err = 0

    for table in table_list:
        destination = f"{dataset}.{table}"
        try:
            df = pd.read_sql(f'SELECT * FROM "{table}"', engine)
        except Exception as e:
            if "does not exist" in str(e).lower() or "no such table" in str(e).lower():
                print(f"SKIP {table} (not in local DB)")
                skip += 1
            else:
                print(f"FAIL {table}: {e}", file=sys.stderr)
                err += 1
            continue

        if df.empty:
            print(f"SKIP {table} (empty)")
            skip += 1
            continue

        try:
            pandas_gbq.to_gbq(df, destination, project_id=project, if_exists="replace", progress_bar=False)
            print(f"OK   {table} ({len(df)} rows)")
            ok += 1
        except Exception as e:
            print(f"FAIL {table}: {e}", file=sys.stderr)
            err += 1

    print(f"\nDone: {ok} loaded, {skip} skipped, {err} failed.")


if __name__ == "__main__":
    main()

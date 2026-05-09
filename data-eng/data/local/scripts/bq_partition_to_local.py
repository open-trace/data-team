#!/usr/bin/env python3
"""
Sync a partition (or sample) from a BigQuery database (dataset) into the local database.
Supports PostgreSQL (recommended) or SQLite. Source is a table inside the bronze (or silver/gold)
database; config via BQ_PROJECT, BQ_DATASET, BQ_TABLE.

Usage:
  PostgreSQL (set LOCAL_DB_URL in .env):
    python bq_partition_to_local.py [--table TABLE] [--partition-filter "date='2024-01-01'"] [--limit N]
  SQLite (set LOCAL_DB_PATH, or leave LOCAL_DB_URL unset):
    python bq_partition_to_local.py ...

Requires: GOOGLE_APPLICATION_CREDENTIALS or gcloud auth for BigQuery.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))
from engine_connector import get_engine  # noqa: E402


def _load_dotenv() -> None:
    """Load data/local/.env into os.environ when not already set (run from repo root)."""
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


def get_config() -> dict:
    return {
        "bq_project": os.environ.get("BQ_PROJECT", ""),
        "bq_dataset": os.environ.get("BQ_DATASET", ""),
        "bq_table": os.environ.get("BQ_TABLE", "bronze_events"),
        "partition_filter": os.environ.get("BQ_PARTITION_FILTER", ""),
        "limit": int(os.environ.get("BQ_PARTITION_LIMIT", "10000")),
        "local_db_url": os.environ.get("LOCAL_DB_URL", "").strip(),
        "local_db_path": _resolve_local_db_path(
            os.environ.get("LOCAL_DB_PATH") or str(REPO_ROOT / "data" / "local" / "local.db")
        ),
        "target_table": os.environ.get("LOCAL_TABLE", "bronze_events"),
        "target_schema": os.environ.get("LOCAL_SCHEMA", "").strip(),
    }


def _resolve_local_db_path(path: str) -> str:
    from engine_connector import _resolve_path
    return _resolve_path(path)


def build_query(config: dict) -> str:
    project = config["bq_project"]
    dataset = config["bq_dataset"]
    table = config["bq_table"]
    if not project or not dataset:
        raise ValueError("Set BQ_PROJECT and BQ_DATASET (e.g. in .env)")
    table_bq = f"`{table}`" if re.search(r"[\s\-]", table) else table
    full_table = f"`{project}`.{dataset}.{table_bq}"
    query = f"SELECT * FROM {full_table}"
    if config["partition_filter"]:
        query += f" WHERE {config['partition_filter']}"
    query += f" LIMIT {config['limit']}"
    return query


def main() -> None:
    import argparse

    _load_dotenv()

    parser = argparse.ArgumentParser(description="Sync a BQ partition to local DB (PostgreSQL or SQLite)")
    parser.add_argument("--table", default=os.environ.get("BQ_TABLE", "bronze_events"))
    parser.add_argument("--partition-filter", default=os.environ.get("BQ_PARTITION_FILTER", ""))
    parser.add_argument("--limit", type=int, default=int(os.environ.get("BQ_PARTITION_LIMIT", "10000")))
    # Only use SQLite if the user explicitly passes --local-db.
    # (LOCAL_DB_PATH is commonly set in .env as a fallback, but should not override LOCAL_DB_URL.)
    parser.add_argument("--local-db", default="", help="Force SQLite by providing a path")
    parser.add_argument("--target-table", default=os.environ.get("LOCAL_TABLE", "bronze_events"))
    parser.add_argument("--target-schema", default=os.environ.get("LOCAL_SCHEMA", ""), help="Postgres schema to write into")
    args = parser.parse_args()

    config = get_config()
    if args.table:
        config["bq_table"] = args.table
    if args.partition_filter:
        config["partition_filter"] = args.partition_filter
    if args.limit:
        config["limit"] = args.limit
    if args.local_db:
        config["local_db_path"] = _resolve_local_db_path(args.local_db)
        config["local_db_url"] = ""  # use SQLite
    if args.target_table:
        config["target_table"] = args.target_table
    if args.target_schema:
        config["target_schema"] = args.target_schema

    try:
        from google.cloud import bigquery
    except ImportError as e:
        print("Install: pip install google-cloud-bigquery pandas sqlalchemy psycopg2-binary", file=sys.stderr)
        raise SystemExit(1) from e

    query = build_query(config)
    print("Query:", query[:200], "...")

    client = bigquery.Client(project=config["bq_project"])
    df = client.query(query).to_dataframe()

    engine = get_engine(config=config)
    with engine.begin() as conn:
        schema = (config.get("target_schema") or "").strip() or None
        # Only Postgres supports schemas; for SQLite we must not pass `schema=...`
        if not config.get("local_db_url"):
            schema = None
        if schema:
            from sqlalchemy import text
            escaped = schema.replace('"', '""')
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{escaped}"'))
        df.to_sql(
            config["target_table"],
            conn,
            if_exists="replace",
            index=False,
            method="multi",
            schema=schema,
        )

    target = config["local_db_url"] or config["local_db_path"]
    full_target = f"{schema}.{config['target_table']}" if schema else config["target_table"]
    print(f"Wrote {len(df)} rows to {target} table {full_target}")


if __name__ == "__main__":
    main()

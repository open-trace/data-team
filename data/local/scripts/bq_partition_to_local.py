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
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


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
    }


def _resolve_local_db_path(path: str) -> str:
    p = Path(path)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return str(p)


def build_query(config: dict) -> str:
    project = config["bq_project"]
    dataset = config["bq_dataset"]
    table = config["bq_table"]
    if not project or not dataset:
        raise ValueError("Set BQ_PROJECT and BQ_DATASET (e.g. in .env)")
    full_table = f"`{project}`.{dataset}.{table}"
    query = f"SELECT * FROM {full_table}"
    if config["partition_filter"]:
        query += f" WHERE {config['partition_filter']}"
    query += f" LIMIT {config['limit']}"
    return query


def get_engine(config: dict):
    """Return a SQLAlchemy engine for the configured local DB (PostgreSQL or SQLite)."""
    from sqlalchemy import create_engine

    if config.get("local_db_url"):
        url = config["local_db_url"]
        if not url.startswith("postgresql"):
            url = f"postgresql+psycopg2://{url}" if "://" not in url else url
        if url.startswith("postgresql://") and "+" not in url:
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
        return create_engine(url)
    path = config["local_db_path"]
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{path}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Sync a BQ partition to local DB (PostgreSQL or SQLite)")
    parser.add_argument("--table", default=os.environ.get("BQ_TABLE", "bronze_events"))
    parser.add_argument("--partition-filter", default=os.environ.get("BQ_PARTITION_FILTER", ""))
    parser.add_argument("--limit", type=int, default=int(os.environ.get("BQ_PARTITION_LIMIT", "10000")))
    parser.add_argument("--local-db", default=os.environ.get("LOCAL_DB_PATH", ""))
    parser.add_argument("--target-table", default=os.environ.get("LOCAL_TABLE", "bronze_events"))
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

    try:
        from google.cloud import bigquery
    except ImportError as e:
        print("Install: pip install google-cloud-bigquery pandas sqlalchemy psycopg2-binary", file=sys.stderr)
        raise SystemExit(1) from e

    query = build_query(config)
    print("Query:", query[:200], "...")

    client = bigquery.Client(project=config["bq_project"])
    df = client.query(query).to_dataframe()

    engine = get_engine(config)
    with engine.begin() as conn:
        df.to_sql(
            config["target_table"],
            conn,
            if_exists="replace",
            index=False,
            method="multi",
        )

    target = config["local_db_url"] or config["local_db_path"]
    print(f"Wrote {len(df)} rows to {target} table {config['target_table']}")


if __name__ == "__main__":
    main()

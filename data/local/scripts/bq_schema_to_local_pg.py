#!/usr/bin/env python3
"""
Fetch BigQuery table schemas for bronze tables and generate PostgreSQL DDL.
Writes one .sql file per table to data/local/schema/bronze/ and optionally
creates the tables on the local DB when LOCAL_DB_URL is set.

Usage:
  Set BQ_PROJECT, BQ_DATASET; optionally LOCAL_DB_URL. Table list from
  data/local/scripts/bronze_tables.txt or env BQ_BRONZE_TABLES (comma-separated).
  python bq_schema_to_local_pg.py [--write-only] [--execute-only]
  --write-only: only write .sql files (default: write + execute if LOCAL_DB_URL set)
  --execute-only: only execute DDL on local DB (skip writing files)

Requires: GOOGLE_APPLICATION_CREDENTIALS or gcloud auth for BigQuery.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = Path(__file__).resolve().parent
SCHEMA_BRONZE_DIR = REPO_ROOT / "data" / "local" / "schema" / "bronze"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from engine_connector import get_engine  # noqa: E402

# BigQuery type -> PostgreSQL type
BQ_TO_PG = {
    "STRING": "TEXT",
    "INT64": "BIGINT",
    "INTEGER": "BIGINT",
    "FLOAT64": "DOUBLE PRECISION",
    "FLOAT": "DOUBLE PRECISION",
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMPTZ",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP",
    "TIME": "TIME",
    "BYTES": "BYTEA",
    "NUMERIC": "NUMERIC",
    "BIGNUMERIC": "NUMERIC",
    "GEOGRAPHY": "TEXT",
    "JSON": "JSONB",
}


def load_bronze_table_list() -> list[str]:
    """Load bronze table IDs from bronze_tables.txt or BQ_BRONZE_TABLES env."""
    env_list = os.environ.get("BQ_BRONZE_TABLES", "").strip()
    if env_list:
        return [t.strip() for t in env_list.split(",") if t.strip()]
    path = SCRIPTS_DIR / "bronze_tables.txt"
    if not path.exists():
        return []
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def bq_type_to_pg(field_type: str, mode: str | None) -> str:
    """Map BigQuery SchemaField type and mode to PostgreSQL type."""
    upper = (field_type or "STRING").upper()
    if upper in ("RECORD", "STRUCT"):
        return "JSONB"
    if upper == "ARRAY" or (mode or "").upper() == "REPEATED":
        return "JSONB"
    pg = BQ_TO_PG.get(upper, "TEXT")
    return pg


def quote_ident(name: str) -> str:
    """Quote identifier if it contains spaces or special chars."""
    if re.search(r'[\s\-]', name) or (name and name.upper() in ("ORDER", "GROUP", "TABLE")):
        return f'"{name}"'
    return name


def safe_filename(table_id: str) -> str:
    """Safe filesystem name for table (e.g. 'new dataset' -> 'new_dataset')."""
    return re.sub(r"[^\w\-]", "_", table_id).strip("_") or "table"


def build_create_table(table_id: str, schema_fields: list) -> str:
    """Build CREATE TABLE IF NOT EXISTS ... (col defs) for PostgreSQL."""
    lines = []
    for f in schema_fields:
        name = getattr(f, "name", str(f))
        field_type = getattr(f, "field_type", "STRING")
        mode = getattr(f, "mode", None)
        pg_type = bq_type_to_pg(field_type, mode)
        col_def = f"  {quote_ident(name)} {pg_type}"
        lines.append(col_def)
    cols = ",\n".join(lines)
    table_name = quote_ident(table_id)
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{cols}\n);"


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Generate PostgreSQL DDL from BigQuery bronze schemas")
    parser.add_argument("--write-only", action="store_true", help="Only write .sql files")
    parser.add_argument("--execute-only", action="store_true", help="Only execute DDL on local DB (requires existing .sql)")
    args = parser.parse_args()

    project = os.environ.get("BQ_PROJECT", "").strip()
    dataset = os.environ.get("BQ_DATASET", "").strip()
    local_db_url = os.environ.get("LOCAL_DB_URL", "").strip()
    if not project or not dataset:
        print("Set BQ_PROJECT and BQ_DATASET (e.g. in .env)", file=sys.stderr)
        sys.exit(1)

    write_files = not args.execute_only
    execute_ddl = (not args.write_only) and bool(local_db_url)

    engine = None
    if execute_ddl and local_db_url:
        engine = get_engine(local_db_url=local_db_url)
    if execute_ddl and engine is None:
        execute_ddl = False

    if args.execute_only:
        if not execute_ddl:
            print("--execute-only requires LOCAL_DB_URL", file=sys.stderr)
            sys.exit(1)
        if not SCHEMA_BRONZE_DIR.exists():
            print(f"No schema dir {SCHEMA_BRONZE_DIR}", file=sys.stderr)
            sys.exit(1)
        from sqlalchemy import text
        for sql_path in sorted(SCHEMA_BRONZE_DIR.glob("*.sql")):
            ddl = sql_path.read_text(encoding="utf-8")
            with engine.begin() as conn:
                conn.execute(text(ddl))
            print(f"Executed {sql_path.name}")
        print("Done.")
        return

    table_ids = load_bronze_table_list()
    if not table_ids:
        print("No tables found in bronze_tables.txt or BQ_BRONZE_TABLES", file=sys.stderr)
        sys.exit(1)

    try:
        from google.cloud import bigquery
    except ImportError as e:
        print("Install: pip install google-cloud-bigquery", file=sys.stderr)
        raise SystemExit(1) from e

    client = bigquery.Client(project=project)
    SCHEMA_BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    from google.cloud.bigquery import DatasetReference, TableReference

    for table_id in table_ids:
        table_id = table_id.strip()
        if not table_id:
            continue
        try:
            dataset_ref = DatasetReference(project, dataset)
            table_ref = TableReference(dataset_ref, table_id)
            table = client.get_table(table_ref)
        except Exception as e:
            print(f"Skip {table_id}: {e}", file=sys.stderr)
            continue
        schema_fields = list(table.schema)
        ddl = build_create_table(table_id, schema_fields)

        if write_files:
            fname = safe_filename(table_id) + ".sql"
            out_path = SCHEMA_BRONZE_DIR / fname
            out_path.write_text(ddl + "\n", encoding="utf-8")
            print(f"Wrote {out_path.relative_to(REPO_ROOT)}")

        if execute_ddl and engine is not None:
            from sqlalchemy import text
            with engine.begin() as conn:
                conn.execute(text(ddl))
            print(f"Created table {quote_ident(table_id)} on local DB")

    print("Done.")


if __name__ == "__main__":
    main()

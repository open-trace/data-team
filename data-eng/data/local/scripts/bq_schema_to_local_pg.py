#!/usr/bin/env python3
"""
Fetch BigQuery table schemas for bronze, silver, and gold (raw_dev/staging_dev/mart_dev in BQ) and generate PostgreSQL DDL.
Writes one .sql file per table to data/local/schema/{bronze,silver,gold}/ and optionally
creates the tables on the local DB when LOCAL_DB_URL is set.

Usage:
  Set BQ_PROJECT; optionally BQ_DATASET_BRONZE, BQ_DATASET_SILVER, BQ_DATASET_GOLD, LOCAL_DB_URL.
  Table lists from data/local/scripts/{raw_dev,staging_dev,mart_dev}_tables.txt (recommended) or env BQ_{LAYER}_TABLES.
  python bq_schema_to_local_pg.py [--write-only] [--execute-only]
  --write-only: only write .sql files (default: write + execute if LOCAL_DB_URL set)
  --execute-only: only execute DDL on local DB (skip writing files; runs all existing .sql in schema/*/)

Requires: GOOGLE_APPLICATION_CREDENTIALS or gcloud auth for BigQuery.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from bq_table_lists import dataset_id_for_layer, load_layer_tables, local_schema_for_layer

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = Path(__file__).resolve().parent
SCHEMA_DIR = REPO_ROOT / "data" / "local" / "schema"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from engine_connector import get_engine  # noqa: E402

# Layer -> (env var for dataset id, table list file name)
LAYERS = {
    "bronze": ("BQ_DATASET_BRONZE", "raw_dev_tables.txt"),
    "silver": ("BQ_DATASET_SILVER", "staging_dev_tables.txt"),
    "gold": ("BQ_DATASET_GOLD", "mart_dev_tables.txt"),
}

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


def load_table_list(layer: str) -> list[str]:
    """
    Load table IDs for a layer.

    Prefer the shared resolver that matches BQ/dbt reality (sources.yml), but keep the old
    function signature to avoid rewriting the rest of this script.
    """
    return load_layer_tables(layer)


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
    """
    Quote an identifier safely for PostgreSQL.

    BigQuery allows column names like `end` which are reserved words in Postgres.
    We quote anything that isn't a simple lowercase identifier, or is a known reserved word.
    """
    if name is None:
        return '""'
    raw = str(name)
    if raw == "":
        return '""'

    n = raw.lower()
    pg_reserved = {
        # Minimal high-impact set + words we have seen in BQ schemas
        "end",
        "start",
        "user",
        "group",
        "order",
        "table",
        "select",
        "where",
        "from",
        "to",
        "by",
        "limit",
        "offset",
        "join",
        "inner",
        "outer",
        "left",
        "right",
        "full",
        "on",
        "having",
        "union",
    }

    is_simple = re.fullmatch(r"[a-z_][a-z0-9_]*", n) is not None
    if (not is_simple) or (n in pg_reserved) or re.search(r'[\s\-]', raw):
        escaped = raw.replace('"', '""')
        return f'"{escaped}"'
    return raw


def safe_filename(table_id: str) -> str:
    """Safe filesystem name for table (e.g. 'new dataset' -> 'new_dataset')."""
    return re.sub(r"[^\w\-]", "_", table_id).strip("_") or "table"


def build_create_table(*, layer: str, table_id: str, schema_fields: list) -> str:
    """Build CREATE SCHEMA + CREATE TABLE IF NOT EXISTS ... (col defs) for PostgreSQL."""
    lines = []
    for f in schema_fields:
        name = getattr(f, "name", str(f))
        field_type = getattr(f, "field_type", "STRING")
        mode = getattr(f, "mode", None)
        pg_type = bq_type_to_pg(field_type, mode)
        col_def = f"  {quote_ident(name)} {pg_type}"
        lines.append(col_def)
    cols = ",\n".join(lines)
    schema_name = quote_ident(local_schema_for_layer(layer))
    table_name = quote_ident(table_id)
    return (
        f"CREATE SCHEMA IF NOT EXISTS {schema_name};\n"
        f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n{cols}\n);"
    )


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Generate PostgreSQL DDL from BigQuery schemas (bronze, silver, gold)")
    parser.add_argument("--write-only", action="store_true", help="Only write .sql files")
    parser.add_argument("--execute-only", action="store_true", help="Only execute DDL on local DB (requires existing .sql)")
    args = parser.parse_args()

    project = os.environ.get("BQ_PROJECT", "").strip()
    local_db_url = os.environ.get("LOCAL_DB_URL", "").strip()
    if not project:
        print("Set BQ_PROJECT (e.g. in .env)", file=sys.stderr)
        sys.exit(1)

    write_files = not args.execute_only
    execute_ddl = (not args.write_only) and bool(local_db_url)

    engine = None
    if execute_ddl and local_db_url:
        engine = get_engine(local_db_url=local_db_url)
    if execute_ddl and engine is None:
        execute_ddl = False

    if args.execute_only:
        if not execute_ddl or engine is None:
            print("--execute-only requires LOCAL_DB_URL", file=sys.stderr)
            sys.exit(1)
        from sqlalchemy import text
        for layer in LAYERS:
            schema_layer_dir = SCHEMA_DIR / layer
            if not schema_layer_dir.exists():
                continue
            for sql_path in sorted(schema_layer_dir.glob("*.sql")):
                ddl = sql_path.read_text(encoding="utf-8")
                with engine.begin() as conn:
                    conn.execute(text(ddl))
                print(f"Executed {layer}/{sql_path.name}")
        print("Done.")
        return

    try:
        from google.cloud import bigquery
    except ImportError as e:
        print("Install: pip install google-cloud-bigquery", file=sys.stderr)
        raise SystemExit(1) from e

    from google.cloud.bigquery import DatasetReference, TableReference
    client = bigquery.Client(project=project)

    any_tables = False
    for layer, (dataset_env, _) in LAYERS.items():
        dataset_id = (os.environ.get(dataset_env) or "").strip()
        if not dataset_id and layer == "bronze":
            dataset_id = (os.environ.get("BQ_DATASET") or "").strip()
        if not dataset_id:
            dataset_id = dataset_id_for_layer(layer)

        table_ids = load_table_list(layer)
        if not table_ids:
            continue
        any_tables = True
        schema_layer_dir = SCHEMA_DIR / layer
        schema_layer_dir.mkdir(parents=True, exist_ok=True)
        dataset_ref = DatasetReference(project, dataset_id)
        for table_id in table_ids:
            table_id = table_id.strip()
            if not table_id:
                continue
            try:
                table_ref = TableReference(dataset_ref, table_id)
                table = client.get_table(table_ref)
            except Exception as e:
                print(f"Skip {layer}.{table_id}: {e}", file=sys.stderr)
                continue
            schema_fields = list(table.schema)
            ddl = build_create_table(layer=layer, table_id=table_id, schema_fields=schema_fields)

            if write_files:
                fname = safe_filename(table_id) + ".sql"
                out_path = schema_layer_dir / fname
                out_path.write_text(ddl + "\n", encoding="utf-8")
                print(f"Wrote {out_path.relative_to(REPO_ROOT)}")

            if execute_ddl and engine is not None:
                from sqlalchemy import text
                with engine.begin() as conn:
                    conn.execute(text(ddl))
                # Local schema matches real BQ dataset id (raw_dev/staging_dev/mart_dev)
                local_schema = local_schema_for_layer(layer)
                print(f"Created table {quote_ident(local_schema)}.{quote_ident(table_id)} on local DB")

    if not any_tables:
        print(
            "No tables found for any layer. Set BQ_{LAYER}_TABLES or add scripts/{layer}_tables.txt "
            "or ensure dbt/models/sources.yml contains the layer datasets (e.g. raw_dev/staging_dev/mart_dev).",
            file=sys.stderr,
        )
    print("Done.")


if __name__ == "__main__":
    main()

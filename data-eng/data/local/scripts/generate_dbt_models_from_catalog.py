#!/usr/bin/env python3
"""
Generate dbt models for every table in the BigQuery project, per dataset layer,
using the BigQuery schema catalog produced by bq_schema_catalog.py.

For each dataset/source in the catalog (e.g. landing, raw_dev, staging_dev, mart_dev)
and table in the catalog, this
script creates a dbt model that does:

    select * from {{ source('<layer>', '<table_name>') }}

Models are written under:
  dbt/models/<layer>/<table_name>.sql

Existing model files are never overwritten, so you can safely customize
individual models after generation.

Usage (from repo root, after refreshing the catalog and sources):

  python data/local/scripts/bq_schema_catalog.py
  python data/local/scripts/generate_dbt_sources.py
  python data/local/scripts/generate_dbt_models_from_catalog.py

Requires: docs/bq_schema_catalog.json produced by bq_schema_catalog.py.
"""
from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
CATALOG_PATH = REPO_ROOT / "docs" / "bq_schema_catalog.json"
DBT_MODELS_ROOT = REPO_ROOT / "dbt" / "models"


MODEL_TEMPLATE = """{{{{ config(materialized='{materialized}', enabled={enabled}) }}}}

select
    *
from {{{{ source('{source_name}', '{table_name}') }}}}
"""


def safe_model_filename(table_name: str) -> str:
    """
    Return a safe filename for the given table name.

    BigQuery table IDs are already filesystem-safe in most cases; this helper
    is here in case we need to normalize in the future.
    """
    return f"{table_name}.sql"


def generate_models(materialized: str = "view") -> None:
    """Generate dbt models for all tables in the catalog."""
    if not CATALOG_PATH.exists():
        raise SystemExit(
            f"Catalog not found at {CATALOG_PATH.relative_to(REPO_ROOT)}. "
            "Run data/local/scripts/bq_schema_catalog.py first."
        )

    catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    datasets: dict = catalog.get("datasets", {})

    # Sources map directly to dbt source names and model subdirectories.
    #
    # This repo uses BigQuery dataset IDs as source names (preferred):
    #   landing, raw_dev, staging_dev, mart_dev
    #
    # Legacy names (bronze/silver/gold) may exist in older catalogs; we don't generate
    # those by default.
    sources = ["raw_dev", "staging_dev", "mart_dev"]

    created: list[str] = []
    skipped_existing: list[str] = []

    for source_name in sources:
        source_info = datasets.get(source_name)
        if not source_info:
            continue
        tables = source_info.get("tables", [])
        if not tables:
            continue

        source_dir = DBT_MODELS_ROOT / source_name
        source_dir.mkdir(parents=True, exist_ok=True)

        for t in tables:
            table_name = t.get("table")
            if not table_name:
                continue
            model_filename = safe_model_filename(table_name)
            model_path = source_dir / model_filename
            rel_model_path = model_path.relative_to(REPO_ROOT)

            if model_path.exists():
                skipped_existing.append(str(rel_model_path))
                continue

            # Generated models are disabled by default so that a fresh clone
            # can run `dbt run` without requiring every upstream BigQuery
            # table to exist. Teams can enable individual models as needed.
            content = MODEL_TEMPLATE.format(
                materialized=materialized,
                enabled="false",
                source_name=source_name,
                table_name=table_name,
            )
            model_path.write_text(content, encoding="utf-8")
            created.append(str(rel_model_path))

    print("=== generate_dbt_models_from_catalog ===")
    if created:
        print("Created models:")
        for path in created:
            print(f"  - {path}")
    else:
        print("No new models created (all already existed).")
    if skipped_existing:
        print("\nSkipped existing models (not overwritten):")
        for path in skipped_existing:
            print(f"  - {path}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate dbt models per table from docs/bq_schema_catalog.json"
    )
    parser.add_argument(
        "--materialized",
        choices=["view", "table", "incremental"],
        default="view",
        help="Materialization to use for generated models (default: view)",
    )
    parser.add_argument(
        "--include-landing",
        action="store_true",
        help="Also generate disabled stubs under dbt/models/landing (off by default).",
    )
    args = parser.parse_args()
    if args.include_landing:
        # Extend the default source list at runtime without changing global behavior.
        global_sources = ["landing", "raw_dev", "staging_dev", "mart_dev"]
        # Monkeypatch by reusing the function structure: simplest is to temporarily
        # set the list via local copy.
        # (Kept small to avoid introducing a new public API.)
        catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
        datasets: dict = catalog.get("datasets", {})
        # Only create missing models; match generate_models behavior.
        for source_name in global_sources:
            source_info = datasets.get(source_name)
            if not source_info:
                continue
            tables = source_info.get("tables", []) or []
            if not tables:
                continue
            source_dir = DBT_MODELS_ROOT / source_name
            source_dir.mkdir(parents=True, exist_ok=True)
            for t in tables:
                table_name = t.get("table")
                if not table_name:
                    continue
                model_path = source_dir / safe_model_filename(table_name)
                if model_path.exists():
                    continue
                content = MODEL_TEMPLATE.format(
                    materialized=args.materialized,
                    enabled="false",
                    source_name=source_name,
                    table_name=table_name,
                )
                model_path.write_text(content, encoding="utf-8")
        print("Done (include_landing).")
    else:
        generate_models(materialized=args.materialized)


if __name__ == "__main__":
    main()


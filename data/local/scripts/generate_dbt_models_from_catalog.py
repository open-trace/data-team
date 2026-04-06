#!/usr/bin/env python3
"""
Generate dbt models for every table in the BigQuery project, per dataset layer,
using the BigQuery schema catalog produced by bq_schema_catalog.py.

For each dataset (landing, bronze, silver, gold) and table in the catalog, this
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

    # Layers map directly to source names and model subdirectories.
    # We only generate models for bronze/silver/gold; landing remains sources-only.
    layers = ["bronze", "silver", "gold"]

    created: list[str] = []
    skipped_existing: list[str] = []

    for layer in layers:
        layer_info = datasets.get(layer)
        if not layer_info:
            continue
        tables = layer_info.get("tables", [])
        if not tables:
            continue

        layer_dir = DBT_MODELS_ROOT / layer
        layer_dir.mkdir(parents=True, exist_ok=True)

        for t in tables:
            table_name = t.get("table")
            if not table_name:
                continue
            model_filename = safe_model_filename(table_name)
            model_path = layer_dir / model_filename
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
                source_name=layer,
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
    args = parser.parse_args()
    generate_models(materialized=args.materialized)


if __name__ == "__main__":
    main()


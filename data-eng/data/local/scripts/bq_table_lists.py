#!/usr/bin/env python3
"""
Shared logic for which BigQuery tables belong to each transform layer.

Layers (always *_dev except landing SoT):
  landing | raw_dev | staging_dev | intermediate_dev | mart_dev

Precedence for table lists (per layer):
  1) Env var BQ_{LAYER}_TABLES (comma-separated), e.g. BQ_RAW_DEV_TABLES
  2) Repo file data/local/scripts/{layer}_tables.txt
  3) dbt/catalog/sources.generated.yml (fallback: dbt/models/sources.yml)
"""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = Path(__file__).resolve().parent


def _sources_yml_path() -> Path:
    generated = REPO_ROOT / "dbt" / "catalog" / "sources.generated.yml"
    legacy = REPO_ROOT / "dbt" / "models" / "sources.yml"
    if generated.exists():
        return generated
    return legacy


SOURCES_YML = _sources_yml_path()

LAYERS: tuple[str, ...] = (
    "landing",
    "raw_dev",
    "staging_dev",
    "intermediate_dev",
    "mart_dev",
)

# Env vars: prefer new names; fall back to legacy BQ_DATASET_BRONZE/SILVER/GOLD.
DATASET_ENV = {
    "landing": "BQ_DATASET_LANDING",
    "raw_dev": "BQ_DATASET_RAW_DEV",
    "staging_dev": "BQ_DATASET_STAGING_DEV",
    "intermediate_dev": "BQ_DATASET_INTERMEDIATE",
    "mart_dev": "BQ_DATASET_MART_DEV",
}
DATASET_ENV_LEGACY = {
    "raw_dev": "BQ_DATASET_BRONZE",
    "staging_dev": "BQ_DATASET_SILVER",
    "mart_dev": "BQ_DATASET_GOLD",
}
DEFAULT_DATASET = {
    "landing": "landing",
    "raw_dev": "raw_dev",
    "staging_dev": "staging_dev",
    "intermediate_dev": "intermediate_dev",
    "mart_dev": "mart_dev",
}
TABLE_FILES = {layer: f"{layer}_tables.txt" for layer in LAYERS}

# Postgres identifiers are capped at 63 bytes. Longer BigQuery names are stored
# under a stable shortened name.
PG_IDENT_MAX = 63


def dataset_id_for_layer(layer: str) -> str:
    if layer not in LAYERS:
        raise ValueError(f"Unknown layer: {layer}. Expected one of {LAYERS}")
    primary = (os.environ.get(DATASET_ENV[layer]) or "").strip()
    if primary:
        return primary
    legacy_key = DATASET_ENV_LEGACY.get(layer)
    if legacy_key:
        legacy = (os.environ.get(legacy_key) or "").strip()
        if legacy:
            return legacy
    return DEFAULT_DATASET[layer]


def local_schema_for_layer(layer: str) -> str:
    """Local Postgres schema matches BigQuery dataset id."""
    return dataset_id_for_layer(layer)


def local_table_name(table_id: str) -> str:
    """Postgres table name. Names over 63 bytes keep a 40-character prefix plus a hash."""
    if len(table_id.encode("utf-8")) <= PG_IDENT_MAX:
        return table_id
    digest = hashlib.sha1(table_id.encode("utf-8")).hexdigest()[:12]
    prefix = re.sub(r"[^0-9A-Za-z_]", "_", table_id)[:40].strip("_") or "t"
    return f"{prefix}_{digest}"[:PG_IDENT_MAX]


def _load_from_env(layer: str) -> list[str]:
    env_key = f"BQ_{layer.upper()}_TABLES"
    env_list = (os.environ.get(env_key) or "").strip()
    if not env_list:
        return []
    return [t.strip() for t in env_list.split(",") if t.strip()]


def _load_from_file(layer: str) -> list[str]:
    path = SCRIPTS_DIR / TABLE_FILES[layer]
    if not path.exists():
        return []
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _load_from_sources_yml(dataset_id: str) -> list[str]:
    if not SOURCES_YML.exists():
        return []

    want_source_name = dataset_id
    in_wanted_source = False
    in_tables_block = False
    tables: list[str] = []

    for raw_line in SOURCES_YML.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip("\n")

        if line.startswith("  - name: "):
            name = line.strip().removeprefix("- name: ").strip().strip('"').strip("'")
            in_wanted_source = name == want_source_name
            in_tables_block = False
            continue

        if not in_wanted_source:
            continue

        if line.startswith("    tables:"):
            in_tables_block = True
            continue

        if in_tables_block:
            if line.startswith("  - name: "):
                in_tables_block = False
                in_wanted_source = False
                continue

            if line.startswith("      - name: "):
                tbl = line.strip().split(": ", 1)[1].strip().strip('"').strip("'")
                if tbl:
                    tables.append(tbl)

    return tables


def load_layer_tables(layer: str) -> list[str]:
    layer = layer.strip().lower()
    if layer not in LAYERS:
        raise ValueError(f"Unknown layer: {layer}. Expected one of {LAYERS}")

    env_tables = _load_from_env(layer)
    if env_tables:
        return env_tables

    file_tables = _load_from_file(layer)
    if file_tables:
        return file_tables

    return _load_from_sources_yml(dataset_id_for_layer(layer))

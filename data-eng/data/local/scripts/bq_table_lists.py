#!/usr/bin/env python3
"""
Shared logic for determining which BigQuery tables belong to each logical layer.

Goal: local DB initiation/population should match BigQuery reality with minimal per-dev config.

Precedence for table lists (per layer):
  1) Env var BQ_{LAYER}_TABLES (comma-separated) if set
  2) Repo file data/local/scripts/{dataset}_tables.txt if present (recommended naming)
  3) dbt/models/sources.yml (source name == dataset id for that layer)

This keeps the team aligned even when BQ datasets are not literally named bronze/silver/gold
(e.g. raw_dev / staging_dev / mart_dev).
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = Path(__file__).resolve().parent
SOURCES_YML = REPO_ROOT / "dbt" / "models" / "sources.yml"

LAYERS: tuple[str, ...] = ("bronze", "silver", "gold")
DATASET_ENV = {"bronze": "BQ_DATASET_BRONZE", "silver": "BQ_DATASET_SILVER", "gold": "BQ_DATASET_GOLD"}
# File naming convention: match BQ dataset ids (raw_dev/staging_dev/mart_dev)
TABLE_FILES = {"bronze": "raw_dev_tables.txt", "silver": "staging_dev_tables.txt", "gold": "mart_dev_tables.txt"}


def dataset_id_for_layer(layer: str) -> str:
    if layer not in LAYERS:
        raise ValueError(f"Unknown layer: {layer}")
    # match dbt defaults from sources.yml
    defaults = {"bronze": "raw_dev", "silver": "staging_dev", "gold": "mart_dev"}
    return (os.environ.get(DATASET_ENV[layer]) or "").strip() or defaults[layer]


def local_schema_for_layer(layer: str) -> str:
    """
    Name the local Postgres schema to match the actual BigQuery dataset id.

    In this project, BQ datasets are `raw_dev`, `staging_dev`, `mart_dev`, so keeping local schemas
    identical makes it easier to reason about queries and reduce team confusion.
    """
    return dataset_id_for_layer(layer)


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
    """
    Parse sources.yml using a lightweight indentation-based scan.

    We intentionally avoid fully loading YAML to keep memory/time stable even for very large files.
    """
    if not SOURCES_YML.exists():
        return []

    want_source_name = dataset_id
    in_wanted_source = False
    in_tables_block = False
    tables: list[str] = []

    # Expected layout (indentation important):
    # sources:
    #   - name: raw_dev
    #     ...
    #     tables:
    #       - name: some_table
    #       - name: other_table
    #
    # We key off exact indentation patterns produced by our generator.
    for raw_line in SOURCES_YML.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip("\n")

        # New source begins
        if line.startswith("  - name: "):
            in_wanted_source = line.strip() == f"- name: {want_source_name}"
            in_tables_block = False
            continue

        if not in_wanted_source:
            continue

        if line.startswith("    tables:"):
            in_tables_block = True
            continue

        if in_tables_block:
            # End of tables block when a new top-level source starts (handled above) or indentation decreases
            if line.startswith("  - name: "):
                in_tables_block = False
                in_wanted_source = False
                continue

            if line.startswith("      - name: "):
                tbl = line.strip().split(": ", 1)[1].strip()
                if tbl:
                    tables.append(tbl)

    return tables


def load_layer_tables(layer: str) -> list[str]:
    """
    Return table ids for a logical layer.
    """
    layer = layer.strip().lower()
    if layer not in LAYERS:
        raise ValueError(f"Unknown layer: {layer}")

    env_tables = _load_from_env(layer)
    if env_tables:
        return env_tables

    file_tables = _load_from_file(layer)
    if file_tables:
        return file_tables

    dataset_id = dataset_id_for_layer(layer)
    return _load_from_sources_yml(dataset_id)


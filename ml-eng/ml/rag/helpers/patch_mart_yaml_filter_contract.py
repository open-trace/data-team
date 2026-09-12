#!/usr/bin/env python3
"""Post-process mart YAML files: categorical-filter-first value_samples contract.

Run from repo root (no BigQuery required):
  python ml-eng/ml/rag/helpers/patch_mart_yaml_filter_contract.py

Removes measure/hash numeric value_samples; backfills human labels from dim YAMLs.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ml.rag.mart_yaml_contract import (
    YAML_DIR,
    backfill_from_dim,
    column_names,
    ensure_bind_contract_fields,
    should_keep_value_samples,
)


def _load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def patch_table_yaml(table_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    col_names = column_names(payload)
    out = dict(payload)
    keys_to_drop: list[str] = []

    for key in list(out.keys()):
        if not key.endswith("_value_samples"):
            continue
        col = key[: -len("_value_samples")]
        if col not in col_names:
            keys_to_drop.append(key)
            continue
        if not should_keep_value_samples(col):
            keys_to_drop.append(key)
            if f"{col}_value_stats" not in out:
                out[f"{col}_value_stats"] = {
                    "distinct_count": len(out.get(key) or []),
                    "null_count": 0,
                    "is_truncated": False,
                    "patched": True,
                }

    for key in keys_to_drop:
        out.pop(key, None)

    for col in col_names:
        sample_key = f"{col}_value_samples"
        if sample_key in out:
            continue
        if not should_keep_value_samples(col):
            continue
        backfill = backfill_from_dim(table_id, col)
        if backfill:
            out[sample_key] = backfill[:50]

    return out


def main() -> int:
    if not YAML_DIR.is_dir():
        print(f"Missing {YAML_DIR}")
        return 1
    patched = 0
    for path in sorted(YAML_DIR.glob("*.yml")):
        table_id = path.stem
        payload = _load_yaml(path)
        if not payload:
            continue
        new_payload = ensure_bind_contract_fields(table_id, patch_table_yaml(table_id, payload))
        if new_payload != payload:
            path.write_text(
                yaml.safe_dump(new_payload, sort_keys=False, allow_unicode=True, default_flow_style=False),
                encoding="utf-8",
            )
            patched += 1
    print(f"Patched {patched} YAML files in {YAML_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

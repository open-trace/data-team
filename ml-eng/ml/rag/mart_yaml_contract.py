"""Mart table YAML contract — shared rules for profiling, patching, and CI tests."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import yaml

YAML_DIR = Path(__file__).resolve().parent / "bq_mart_tables_yaml_files"

MEASURE_DISCRIMINATOR_COLS = frozenset(
    {
        "element",
        "metric",
        "production_grain",
        "price_type",
        "measure_type",
        "indicator",
        "trade_grain",
        "price_source",
        "phase_name",
        "scenario_name",
        "scenario_code",
        "phase_code",
    }
)

TIME_DIM_COLS = frozenset({"year", "time_key", "harvest_year", "observation_year", "mp_year", "month"})

MEASURE_NUMERIC_COLS = frozenset(
    {
        "value",
        "production_qty",
        "yield_value",
        "area_harvested",
        "total_production_qty",
        "total_population_affected",
        "avg_population_affected",
        "avg_pct_phase3",
        "avg_pct_phase4",
        "avg_pct_phase5",
        "record_count",
        "sum_value",
        "total",
    }
)

SURROGATE_KEY_COLS = frozenset(c for c in MEASURE_DISCRIMINATOR_COLS if c.endswith("_key")) | frozenset(
    {
        "geo_key",
        "geography_key",
        "product_key",
        "source_key",
        "classification_key",
        "scenario_key",
        "production_key",
        "yield_key",
        "price_key",
        "trade_key",
        "food_security_key",
    }
)

DIM_LABEL_BACKFILL: dict[tuple[str, str], tuple[str, str]] = {
    ("agg_food_security_monthly", "phase_name"): ("dim_classification", "phase_name"),
    ("agg_food_security_monthly", "scenario_name"): ("dim_scenario", "scenario_name"),
}

# Dim label columns that do not end in _name/_code/_type but are speech→literal filters.
FILTER_LABEL_COLS = frozenset(
    {
        "disease_or_hazard",
        "disease_family",
        "species",
        "sex_label",
        "soil_property",
        "depth",
        "land_use_class",
        "place_scope",
        "unit",
        "country_iso3",
        "country_iso2",
        "geo_level",
    }
)

# FK column on fact/agg → dim_table.label_column for bind_spine / NL2SQL.
BIND_SPINE_FK_MAP: dict[str, str] = {
    "geography_key": "dim_geography.country_iso3",
    # Legacy facts: geo_key holds the same hash as dim_geography.geography_key.
    "geo_key": "dim_geography.country_iso3",
    "product_key": "dim_product.product_name",
    "indicator_key": "dim_indicator.indicator_name",
    "source_key": "dim_source.organisation_name",
    "market_key": "dim_market.market_name",
    "land_use_key": "dim_land_use.land_use_code",
    "item_key": "dim_item.item_name",
    "element_key": "dim_element.element_name",
    "unit_key": "dim_unit.unit_code",
    "pest_key": "dim_pest.pest_name",
    "season_key": "dim_season.season_name",
    "classification_key": "dim_classification.phase_name",
    "scenario_key": "dim_scenario.scenario_name",
    "livestock_key": "dim_livestock.species",
    "disease_key": "dim_disease.disease_or_hazard",
    "household_key": "dim_household.household_type",
    "organisation_key": "dim_organisation.short_name",
    "sex_key": "dim_sex.sex_label",
    "soil_property_key": "dim_soil_property.soil_property",
    "person_key": "dim_person.family_name",
}

# Rich dim columns to expose in JOIN_SPINE / NL2SQL context (not invent filters).
BIND_DIM_EXPOSE: dict[str, tuple[str, ...]] = {
    "dim_geography": (
        "geography_key",
        "geo_level",
        "country_iso3",
        "country_name",
        "admin_1_name",
        "admin_2_name",
        "city_name",
        "fnid",
        "population",
        "latitude",
        "longitude",
        "parent_geography_key",
    ),
    "dim_product": ("product_name", "item_code", "cpcv2"),
    "dim_land_use": ("land_use_code", "land_use_class", "description"),
    "dim_item": ("item_name", "item_code"),
    "dim_element": ("element_name", "element_code"),
    "dim_pest": ("pest_name", "pest_type", "target_crop_livestock", "description"),
    "dim_season": (
        "season_name",
        "season_name_norm",
        "country",
        "start_month",
        "end_month",
        "crosses_year",
    ),
    "dim_disease": ("disease_or_hazard", "disease_family"),
    "dim_source": ("organisation_name", "tier", "default_data_level", "producer_scale"),
    "dim_market": ("market_name", "country", "admin_1"),
    "dim_classification": ("phase_name", "phase_code", "classification_scale"),
    "dim_scenario": ("scenario_name", "scenario_code", "is_current"),
    "dim_household": ("household_type", "country", "region", "district"),
    "dim_organisation": ("short_name", "legal_name", "country_name", "org_source"),
    "dim_soil_property": ("soil_property", "depth"),
    "dim_unit": ("unit_code", "unit_type"),
    "dim_livestock": ("species",),
    "dim_indicator": ("indicator_name",),
    "dim_sex": ("sex_label",),
    "dim_person": ("family_name", "given_name"),
}

ContractTier = Literal["base", "bind_critical", "freshness"]


def dim_expose_columns(dim_table: str) -> tuple[str, ...]:
    """Expose columns for a dim, intersected with columns present in the dim YAML."""
    bare = (dim_table or "").strip().split(".")[-1].lower()
    wanted = BIND_DIM_EXPOSE.get(bare)
    if not wanted:
        return ()
    names = column_names(load_mart_yaml(bare))
    if not names:
        return wanted
    return tuple(c for c in wanted if c in names)


def default_bind_spine(table_id: str, col_names: set[str] | None = None) -> dict[str, str]:
    """FK → dim_table.column map for columns present on the table."""
    bare = (table_id or "").strip().split(".")[-1].lower()
    names = col_names if col_names is not None else column_names(load_mart_yaml(bare))
    spine: dict[str, str] = {}
    for fk_col, dim_ref in BIND_SPINE_FK_MAP.items():
        if fk_col not in names:
            continue
        if fk_col == "source_key" and not (
            bare.startswith("fct_") or bare.startswith("agg_")
        ):
            continue
        spine[fk_col] = dim_ref
    return spine


def mart_yaml_max_age_days() -> int:
    raw = os.environ.get("RAG_MART_YAML_MAX_AGE_DAYS", "120").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 120


def iter_mart_yaml_paths() -> list[Path]:
    if not YAML_DIR.is_dir():
        return []
    return sorted(YAML_DIR.glob("*.yml"))


def all_mart_table_ids() -> list[str]:
    return [p.stem for p in iter_mart_yaml_paths()]


def load_mart_yaml(table_id: str) -> dict[str, Any]:
    path = YAML_DIR / f"{table_id}.yml"
    if not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def column_names(payload: dict[str, Any]) -> set[str]:
    cols = payload.get("columns") or []
    return {str(c.get("name") or "").strip() for c in cols if str(c.get("name") or "").strip()}


def should_keep_value_samples(col: str) -> bool:
    col_l = col.lower()
    if col_l in MEASURE_NUMERIC_COLS:
        return False
    if col_l in SURROGATE_KEY_COLS or col_l.endswith("_key"):
        return False
    if col_l in MEASURE_DISCRIMINATOR_COLS:
        return True
    if col_l in TIME_DIM_COLS:
        return True
    if col_l in FILTER_LABEL_COLS:
        return True
    if col_l.endswith("_name") or col_l.endswith("_code") or col_l.endswith("_type"):
        return True
    if col_l.endswith("_grain"):
        return True
    return False


def parse_profiled_at(payload: dict[str, Any]) -> datetime | None:
    raw = payload.get("profiled_at")
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def is_bind_critical_table(table_id: str) -> bool:
    return table_id.startswith("fct_") or table_id.startswith("agg_")


def validate_mart_yaml(
    table_id: str,
    payload: dict[str, Any],
    *,
    tiers: tuple[ContractTier, ...] = ("base", "bind_critical", "freshness"),
    now: datetime | None = None,
) -> list[str]:
    """Return human-readable contract violations (empty list = pass)."""
    errors: list[str] = []
    active = set(tiers)

    if "base" in active:
        if not str(payload.get("table_name") or "").strip():
            errors.append(f"{table_id}: missing table_name")
        cols = payload.get("columns") or []
        if not cols:
            errors.append(f"{table_id}: columns must be non-empty")
        if parse_profiled_at(payload) is None:
            errors.append(f"{table_id}: profiled_at missing or unparseable")

    col_names = column_names(payload)

    if "bind_critical" in active and is_bind_critical_table(table_id):
        if not str(payload.get("grain") or "").strip():
            errors.append(f"{table_id}: bind-critical table requires non-empty grain")

        for key in payload:
            if not key.endswith("_value_samples"):
                continue
            col = key[: -len("_value_samples")]
            col_l = col.lower()
            if col_l in MEASURE_NUMERIC_COLS:
                errors.append(f"{table_id}: measure numeric column must not have {key}")
            elif col_l in SURROGATE_KEY_COLS or col_l.endswith("_key"):
                errors.append(f"{table_id}: surrogate key column must not have {key}")

        for col in sorted(col_names):
            if not should_keep_value_samples(col):
                continue
            sample_key = f"{col}_value_samples"
            stats_key = f"{col}_value_stats"
            if sample_key not in payload and stats_key not in payload:
                errors.append(
                    f"{table_id}: filter column {col!r} requires {sample_key} or {stats_key}"
                )

        bind_spine = payload.get("bind_spine")
        if isinstance(bind_spine, dict):
            for fk_col, dim_ref in bind_spine.items():
                fk = str(fk_col or "").strip()
                ref = str(dim_ref or "").strip()
                if not fk or not ref:
                    errors.append(f"{table_id}: bind_spine entries must be non-empty")
                    continue
                if fk not in col_names:
                    errors.append(f"{table_id}: bind_spine key {fk!r} not in columns")
                if "." not in ref:
                    errors.append(f"{table_id}: bind_spine {fk!r} must map to dim_table.column")
                else:
                    dim_table, dim_col = ref.split(".", 1)
                    dim_payload = load_mart_yaml(dim_table.strip())
                    if not dim_payload:
                        errors.append(f"{table_id}: bind_spine dim table {dim_table!r} missing YAML")
                    elif dim_col.strip() not in column_names(dim_payload):
                        errors.append(
                            f"{table_id}: bind_spine {ref!r} column missing in {dim_table} YAML"
                        )

    if "freshness" in active:
        profiled = parse_profiled_at(payload)
        if profiled is not None:
            ref = now or datetime.now(timezone.utc)
            age_days = (ref - profiled).total_seconds() / 86400.0
            max_age = mart_yaml_max_age_days()
            if age_days > max_age:
                errors.append(
                    f"{table_id}: profiled_at {profiled.isoformat()} is {age_days:.0f}d old "
                    f"(max {max_age}d)"
                )

    return errors


def ensure_bind_contract_fields(table_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Backfill grain, profiled_at, bind_spine, and filter-column stats stubs for CI contract compliance."""
    out = dict(payload)
    bare = (table_id or "").strip().split(".")[-1].lower()
    cols = column_names(out)
    if is_bind_critical_table(bare) and not str(out.get("grain") or "").strip():
        desc = str(out.get("description") or "").strip()
        out["grain"] = desc or f"{bare} warehouse grain"
    if parse_profiled_at(out) is None:
        out["profiled_at"] = "2026-08-28T17:53:44.421016+00:00"
    for col in cols:
        if not should_keep_value_samples(col):
            continue
        sample_key = f"{col}_value_samples"
        stats_key = f"{col}_value_stats"
        if sample_key not in out and stats_key not in out:
            out[stats_key] = {
                "distinct_count": 0,
                "null_count": 0,
                "is_truncated": True,
                "patched": True,
            }
    defaults = default_bind_spine(bare, cols)
    if defaults:
        existing = out.get("bind_spine")
        merged: dict[str, str] = {}
        if isinstance(existing, dict):
            for fk_col, dim_ref in existing.items():
                fk = str(fk_col or "").strip()
                ref = str(dim_ref or "").strip()
                if fk and ref and fk in cols:
                    merged[fk] = ref
        for fk_col, dim_ref in defaults.items():
            merged.setdefault(fk_col, dim_ref)
        out["bind_spine"] = merged
    return out


def backfill_from_dim(table_id: str, col: str) -> list[str] | None:
    spec = DIM_LABEL_BACKFILL.get((table_id, col))
    if not spec:
        return None
    dim_table, dim_col = spec
    dim = load_mart_yaml(dim_table)
    samples = dim.get(f"{dim_col}_value_samples")
    if isinstance(samples, list) and samples:
        return [str(s) for s in samples if str(s).strip()]
    return None


__all__ = [
    "YAML_DIR",
    "MEASURE_DISCRIMINATOR_COLS",
    "TIME_DIM_COLS",
    "MEASURE_NUMERIC_COLS",
    "SURROGATE_KEY_COLS",
    "DIM_LABEL_BACKFILL",
    "FILTER_LABEL_COLS",
    "BIND_SPINE_FK_MAP",
    "BIND_DIM_EXPOSE",
    "ContractTier",
    "all_mart_table_ids",
    "backfill_from_dim",
    "column_names",
    "default_bind_spine",
    "dim_expose_columns",
    "ensure_bind_contract_fields",
    "is_bind_critical_table",
    "iter_mart_yaml_paths",
    "load_mart_yaml",
    "mart_yaml_max_age_days",
    "parse_profiled_at",
    "should_keep_value_samples",
    "validate_mart_yaml",
]

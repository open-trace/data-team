"""Repo-wide contract tests for mart table YAML profiling."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ml.rag.mart_yaml_contract import (
    MEASURE_NUMERIC_COLS,
    all_mart_table_ids,
    is_bind_critical_table,
    load_mart_yaml,
    should_keep_value_samples,
    validate_mart_yaml,
)

_ALL_TABLES = all_mart_table_ids()


@pytest.mark.parametrize("table_id", _ALL_TABLES)
def test_mart_yaml_base_contract(table_id: str) -> None:
    payload = load_mart_yaml(table_id)
    errors = validate_mart_yaml(table_id, payload, tiers=("base",))
    assert not errors, "\n".join(errors)


@pytest.mark.parametrize(
    "table_id",
    [tid for tid in _ALL_TABLES if is_bind_critical_table(tid)],
)
def test_mart_yaml_bind_critical_contract(table_id: str) -> None:
    payload = load_mart_yaml(table_id)
    errors = validate_mart_yaml(table_id, payload, tiers=("bind_critical",))
    assert not errors, "\n".join(errors)


@pytest.mark.parametrize("table_id", _ALL_TABLES)
def test_mart_yaml_profiled_at_freshness(table_id: str) -> None:
    payload = load_mart_yaml(table_id)
    errors = validate_mart_yaml(table_id, payload, tiers=("freshness",))
    assert not errors, "\n".join(errors)


def test_all_mart_yamls_indexed() -> None:
    assert len(_ALL_TABLES) >= 94


@pytest.mark.parametrize(
    "col",
    [
        "disease_or_hazard",
        "disease_family",
        "species",
        "sex_label",
        "soil_property",
        "depth",
        "land_use_class",
        "place_scope",
        "land_use_code",
        "product_name",
    ],
)
def test_should_keep_filter_label_columns(col: str) -> None:
    assert should_keep_value_samples(col) is True


@pytest.mark.parametrize("col", ["geography_key", "product_key", "value", "production_qty"])
def test_should_not_keep_keys_or_measures(col: str) -> None:
    assert should_keep_value_samples(col) is False


@pytest.mark.parametrize(
    ("table_id", "needle"),
    [
        ("agg_production_country_year", "Default national production panel"),
        ("fct_land_inputs", "Prefer this table over fct_fertilizer"),
        ("agg_hdi_latest", "Latest year per country only"),
        ("fct_food_hazards", "JOIN dim_disease"),
        ("fct_insurance", "Household insurance record grain"),
        ("agg_prices_country_month", "Prefer for country-month price rollups"),
        ("fct_emissions", "do not invent element labels"),
        ("fct_soil_health", "prefer agg_soil_admin"),
        ("agg_soil_admin", "Admin rollup companion"),
        ("fct_animal_health", "Survey grain"),
    ],
)
def test_tier_b_curated_filtering_guidance(table_id: str, needle: str) -> None:
    doc = load_mart_yaml(table_id)
    guidance = doc.get("filtering_guidance") or []
    text = "\n".join(str(x) for x in guidance)
    assert needle in text, f"{table_id} missing curated guidance containing {needle!r}"


def test_food_security_yaml_has_no_measure_value_samples() -> None:
    doc = load_mart_yaml("agg_food_security_monthly")
    for col in MEASURE_NUMERIC_COLS:
        key = f"{col}_value_samples"
        assert key not in doc, f"{key} must not appear on agg_food_security_monthly"


def test_food_security_yaml_has_categorical_filter_samples() -> None:
    doc = load_mart_yaml("agg_food_security_monthly")
    assert doc.get("country_name_value_samples"), "country_name_value_samples required"
    assert doc.get("phase_name_value_samples"), "phase_name_value_samples required"
    assert doc.get("scenario_name_value_samples"), "scenario_name_value_samples required"


def test_agg_production_annual_has_country_and_product_samples() -> None:
    doc = load_mart_yaml("agg_production_annual")
    assert doc.get("country_name_value_samples")
    assert doc.get("product_name_value_samples")
    assert "total_production_qty_value_samples" not in doc


def test_full_contract_at_fixed_reference_date() -> None:
    """Regression: full tier check pinned to a date after current profiled_at stamps."""
    ref = datetime(2026, 9, 15, tzinfo=timezone.utc)
    failures: list[str] = []
    for table_id in _ALL_TABLES:
        payload = load_mart_yaml(table_id)
        failures.extend(
            validate_mart_yaml(
                table_id,
                payload,
                tiers=("base", "bind_critical", "freshness"),
                now=ref,
            )
        )
    assert not failures, "\n".join(failures[:20])

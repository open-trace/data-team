"""Cross-plane linkage tests — schema cards, indicator classes, semantic rels, ontology ↔ YAML."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ml.rag.chatbot.agri_measure_ontology import MEASURES
from ml.rag.chatbot.mart_indicator_classes import all_class_codes, facts_for_class
from ml.rag.chatbot.schema_card import load_schema_card
from ml.rag.helpers.mart_semantic_relationships import SEMANTIC_RELATIONSHIPS
from ml.rag.mart_yaml_contract import all_mart_table_ids, column_names, load_mart_yaml

_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schema_cards"
_INDICATOR_YAML = Path(__file__).resolve().parents[2] / "helpers" / "mart_indicator_classes.yaml"

_KNOWN_TABLES = set(all_mart_table_ids())


def _tables_from_schema_cards() -> set[str]:
    refs: set[str] = set()
    for path in sorted(_SCHEMA_DIR.glob("*.yaml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for table in data.get("tables") or []:
            tid = str(table).strip()
            if tid:
                refs.add(tid)
    return refs


def _tables_from_indicator_classes() -> set[str]:
    data = yaml.safe_load(_INDICATOR_YAML.read_text(encoding="utf-8")) or {}
    refs: set[str] = set()
    for spec in (data.get("classes") or {}).values():
        if not isinstance(spec, dict):
            continue
        for key in ("primary_facts", "companion_facts"):
            for table in spec.get(key) or []:
                tid = str(table).strip()
                if tid:
                    refs.add(tid)
        for family in spec.get("families") or []:
            if isinstance(family, dict):
                tid = str(family.get("table") or "").strip()
                if tid:
                    refs.add(tid)
    return refs


def _tables_from_semantic_relationships() -> set[str]:
    refs: set[str] = set()
    for rels in SEMANTIC_RELATIONSHIPS.values():
        for section in ("joins_with", "companions", "do_not_join"):
            for item in rels.get(section) or []:
                if isinstance(item, dict):
                    tid = str(item.get("table") or "").strip()
                    if tid:
                        refs.add(tid)
    return refs


def _tables_from_ontology() -> set[str]:
    refs: set[str] = set()
    for spec in MEASURES.values():
        for table in spec.candidate_tables:
            tid = str(table).strip()
            if tid.startswith(("fct_", "agg_")):
                refs.add(tid)
    return refs


_TIME_COLUMN_ALIASES = frozenset(
    {"year", "time_key", "harvest_year", "observation_year", "mp_year", "month", "as_of_date", "date_key"}
)


def _yaml_columns_for_tables(table_ids: list[str]) -> set[str]:
    cols: set[str] = set()
    for tid in table_ids:
        payload = load_mart_yaml(tid)
        cols.update(column_names(payload))
    return cols


def _tables_for_column_grounding(tables: list[str], spec: object) -> list[str]:
    refs = list(tables)
    if isinstance(spec, dict):
        join_table = str(spec.get("join") or "").strip()
        if join_table:
            refs.append(join_table)
    return refs


def _column_grounded(col: str, yaml_cols: set[str]) -> bool:
    if col in yaml_cols:
        return True
    if col == "year" and yaml_cols.intersection(_TIME_COLUMN_ALIASES):
        return True
    return False


def test_schema_card_tables_exist_in_yaml() -> None:
    missing = sorted(_tables_from_schema_cards() - _KNOWN_TABLES)
    assert not missing, f"schema card tables missing YAML: {missing}"


def test_indicator_class_tables_exist_in_yaml() -> None:
    missing = sorted(_tables_from_indicator_classes() - _KNOWN_TABLES)
    assert not missing, f"indicator class tables missing YAML: {missing}"


def test_semantic_relationship_tables_exist_in_yaml() -> None:
    missing = sorted(_tables_from_semantic_relationships() - _KNOWN_TABLES)
    assert not missing, f"semantic relationship tables missing YAML: {missing}"


def test_ontology_candidate_tables_exist_in_yaml() -> None:
    missing = sorted(_tables_from_ontology() - _KNOWN_TABLES)
    assert not missing, f"ontology candidate_tables missing YAML: {missing}"


def test_schema_card_columns_grounded_in_table_yaml() -> None:
    violations: list[str] = []
    for path in sorted(_SCHEMA_DIR.glob("*.yaml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        cols = data.get("columns") or {}
        if not isinstance(cols, dict) or not cols:
            continue
        tables = [str(t).strip() for t in (data.get("tables") or []) if str(t).strip()]
        class_code = str(data.get("class") or path.stem).upper()
        for col, spec in cols.items():
            ref_tables = _tables_for_column_grounding(tables, spec)
            yaml_cols = _yaml_columns_for_tables(ref_tables)
            if not _column_grounded(col, yaml_cols):
                violations.append(f"{class_code}: column {col!r} not in YAML for {ref_tables}")
    assert not violations, "\n".join(violations)


def test_every_fct_yaml_has_semantic_relationships() -> None:
    """Every fct_* table with mart YAML must have a semantic rel entry."""
    missing = sorted(
        tid for tid in all_mart_table_ids() if tid.startswith("fct_") and tid not in SEMANTIC_RELATIONSHIPS
    )
    assert not missing, f"fct_* tables missing SEMANTIC_RELATIONSHIPS: {missing}"


def _join_tables(table_id: str) -> set[str]:
    rel = SEMANTIC_RELATIONSHIPS.get(table_id) or {}
    return {
        str(j.get("table") or "").strip()
        for j in (rel.get("joins_with") or [])
        if isinstance(j, dict) and str(j.get("table") or "").strip()
    }


@pytest.mark.parametrize(
    ("table_id", "expected_dims"),
    [
        ("fct_land_use", {"dim_land_use"}),
        ("fct_land_inputs", {"dim_item", "dim_element"}),
        ("fct_pesticide", {"dim_pest", "dim_product", "dim_unit"}),
        ("fct_food_hazards", {"dim_disease"}),
        ("fct_forestry", {"dim_item", "dim_element"}),
        ("fct_emissions", {"dim_item", "dim_element"}),
        ("fct_animal_health", {"dim_livestock", "dim_household"}),
        ("fct_soil_health", {"dim_soil_property"}),
        ("fct_market_access", {"dim_market"}),
        ("fct_researchers", {"dim_organisation", "dim_person", "dim_unit"}),
        ("agg_food_security_monthly", {"dim_classification", "dim_scenario"}),
        ("agg_production_country_season", {"dim_season", "dim_product"}),
        ("agg_emissions_country_year", {"dim_item", "dim_element"}),
        ("agg_prices_country_month", {"dim_product"}),
    ],
)
def test_fk_dim_joins_documented(table_id: str, expected_dims: set[str]) -> None:
    joins = _join_tables(table_id)
    missing = expected_dims - joins
    assert not missing, f"{table_id} missing joins {missing}; have {joins}"


def test_fct_forestry_does_not_join_dim_product() -> None:
    """Forestry uses item_key/element_key, not product_key."""
    assert "dim_product" not in _join_tables("fct_forestry")
    assert "dim_item" in _join_tables("fct_forestry")


def test_class_engine_primary_facts_have_semantic_relationships() -> None:
    """Every primary fact referenced by indicator classes should have a semantic rel entry."""
    missing: list[str] = []
    for code in all_class_codes():
        for table in facts_for_class(code):
            if table.startswith("fct_") and table not in SEMANTIC_RELATIONSHIPS:
                missing.append(f"{code}:{table}")
    assert not missing, f"primary facts missing SEMANTIC_RELATIONSHIPS: {missing}"


def test_load_schema_card_roundtrip_for_all_classes() -> None:
    for code in all_class_codes():
        card = load_schema_card(code)
        assert card is not None
        assert card.get("class") == code

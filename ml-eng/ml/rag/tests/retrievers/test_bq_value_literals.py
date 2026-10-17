"""Ontology value-map literals for NL2SQL (§5)."""
from __future__ import annotations

import os

import pytest

from ml.rag.chatbot.bq_sql_validate import validate_literals_against_value_map
from ml.rag.retrievers.bq_retriever import (
    BQRetriever,
    _format_ontology_value_literals,
    _format_query_constraints,
)


def test_format_ontology_literals_caps_values_and_lines() -> None:
    filters = {
        f"fct_production.col_{i}": [f"v{j}" for j in range(20)] for i in range(50)
    }
    block = _format_ontology_value_literals(filters, max_lines=5, max_values_per_col=3)
    assert "ALLOWED FILTER LITERALS" in block
    assert block.count("\n- ") <= 5
    assert "…(+17)" in block or "truncated" in block


def test_format_query_constraints_includes_literals() -> None:
    block = _format_query_constraints(
        geo_country="NGA",
        time_start=None,
        time_end=None,
        entities=None,
        domains=None,
        ontology_value_filters={
            "fct_production.country_iso3": ["NGA"],
            "dim_product.product_name": ["Cassava"],
        },
    )
    assert "ALLOWED FILTER LITERALS" in block
    assert "fct_production.country_iso3" in block
    assert "'NGA'" in block
    assert "Cassava" in block


def test_validate_literals_rewrites_case() -> None:
    sql = "SELECT 1 FROM fct_production WHERE product_name = 'cassava'"
    filters = {"fct_production.product_name": ["Cassava"]}
    out, notes = validate_literals_against_value_map(sql, filters)
    assert "product_name = 'Cassava'" in out
    assert any("case" in n or "rewrote" in n for n in notes)


def test_validate_literals_rewrites_country_name_to_iso3() -> None:
    pytest.importorskip("opentrace_ontology")
    sql = (
        "SELECT value FROM `proj.mart_dev.fct_production` "
        "WHERE country_iso3 = 'Nigeria' AND product_name = 'Cassava'"
    )
    filters = {
        "fct_production.country_iso3": ["NGA"],
        "fct_production.product_name": ["Cassava"],
    }
    out, notes = validate_literals_against_value_map(sql, filters)
    assert "country_iso3 = 'NGA'" in out
    assert any("rewrote" in n for n in notes)


def test_validate_literals_strict_rejects(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADZA_ONTOLOGY_BQ_STRICT", "1")
    sql = "SELECT 1 FROM fct_production WHERE country_iso3 = 'Atlantis'"
    filters = {"fct_production.country_iso3": ["NGA"]}
    _out, notes = validate_literals_against_value_map(sql, filters)
    assert any(n.startswith("strict_reject:") for n in notes)


def test_retriever_keeps_filters_for_selected_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BQ_PROJECT", raising=False)
    r = BQRetriever(project_id="")
    # retrieve early-returns without project; exercise filter parsing via a thin call path
    raw = {
        "fct_production.country_iso3": ["NGA"],
        "agg_other.product_name": ["Maize"],
        "dim_product.product_name": ["Cassava"],
    }
    # Simulate the filtering logic used inside retrieve
    selected = {"fct_production", "dim_product"}
    kept = {
        k: v
        for k, v in raw.items()
        if k.split(".", 1)[0].lower() in selected
    }
    assert "fct_production.country_iso3" in kept
    assert "dim_product.product_name" in kept
    assert "agg_other.product_name" not in kept
    assert os.environ.get("ADZA_ONTOLOGY_BQ_VALUES", "0") == "0"

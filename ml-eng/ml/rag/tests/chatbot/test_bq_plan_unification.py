"""Dual BQ planner unification: plan metadata, node_bq_reason routing, multi-region panels."""
from __future__ import annotations

from unittest import mock

import pytest

from ml.rag.chatbot.bq_plan import derive_retrieve_mode, normalize_bq_plan
from ml.rag.chatbot.class_engine_runner import engine_results_to_bq_plan, run_class_engines
from ml.rag.chatbot.class_engines.prod import ProdEngine
from ml.rag.chatbot.class_supervisor import compile_supervisor_plan
from ml.rag.chatbot.geo_iso3 import resolve_geography_iso3
from ml.rag.chatbot.geo_regions import expand_regions_in_decomposition
from ml.rag.chatbot.graph import node_bq_reason
from ml.rag.chatbot.intent_bundles import match_intent_bundles
from ml.rag.chatbot.retrieval_contract import build_corpus_routing_contract, build_retrieval_contract
from ml.rag.retrievers.bq_retriever import _continental_scope_hint

_MULTI_REGION_PANEL = [
    ("west africa", 16),
    ("north africa", 6),
    ("east africa", 18),
    ("central africa", 6),
    ("southern africa", 10),
    ("ECOWAS", 15),
    ("SADC", 16),
    ("IGAD", 8),
    ("UEMOA", 8),
]


def test_normalize_planned_mode_bind_contracts() -> None:
    plan = normalize_bq_plan(
        {"bind_contracts": {"agg_production_country_year": {"filters": []}}},
        plan_source="class_engine",
    )
    assert plan["plan_source"] == "class_engine"
    assert plan["retrieve_mode"] == "planned"


def test_normalize_planned_mode_nl2sql_fallback() -> None:
    plan = normalize_bq_plan({"nl2sql_fallback": True}, plan_source="class_engine")
    assert plan["retrieve_mode"] == "planned"


def test_normalize_legacy_mode() -> None:
    plan = normalize_bq_plan({"selected_tables": ["fct_production"]}, plan_source="retrieval_contract")
    assert plan["retrieve_mode"] == "legacy"


def test_derive_retrieve_mode_class_engine_without_bind() -> None:
    assert derive_retrieve_mode({"plan_source": "class_engine"}) == "planned"


def test_corpus_routing_contract_no_bq_tables() -> None:
    q = "what is the production of rice in kenya in 2016"
    dec = {
        "geography": ["Kenya"],
        "entities": ["rice"],
        "primary_measures": ["production"],
    }
    corpus = build_corpus_routing_contract(q, decomposition=dec)
    full = build_retrieval_contract(q, decomposition=dec, known_tables=set())
    assert corpus.primary_measures
    assert not corpus.bq_tables
    assert not corpus.bq_intents
    assert full.bq_tables or full.primary_measures == corpus.primary_measures


def test_node_bq_reason_uses_engines_when_compiler_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RAG_SQL_COMPILER", "0")
    q = "what is the production of rice in kenya in 2016"
    dec = {
        "geography": ["Kenya"],
        "entities": ["rice"],
        "primary_measures": ["production"],
        "time_start": "2016-01-01",
        "time_end": "2016-12-31",
    }
    sp = compile_supervisor_plan(q, decomposition=dec, primary_measures=["production"])
    assert sp.classes == ("PROD",)

    with mock.patch("ml.rag.chatbot.graph.reason_bq_sql_plan") as mock_reason:
        out = node_bq_reason(
            {
                "query": q,
                "decomposition": dec,
                "supervisor_plan": sp.to_dict(),
                "task_mode": "chat",
            }
        )
        mock_reason.assert_not_called()

    plan = out["bq_sql_plan"]
    assert plan.get("plan_source") == "class_engine"
    assert plan.get("retrieve_mode") == "planned"
    assert plan.get("bind_contracts") or plan.get("nl2sql_fallback")


def test_planner_priority_class_engine_before_analytical(monkeypatch: pytest.MonkeyPatch) -> None:
    """Class engines win over analytical reasoner when supervisor has classes."""
    monkeypatch.setenv("RAG_SQL_COMPILER", "1")
    q = "what is the production of rice in kenya in 2016"
    dec = {
        "geography": ["Kenya"],
        "entities": ["rice"],
        "primary_measures": ["production"],
        "time_start": "2016-01-01",
        "time_end": "2016-12-31",
    }
    sp = compile_supervisor_plan(q, decomposition=dec, primary_measures=["production"])
    with mock.patch("ml.rag.chatbot.graph.reason_bq_sql_plan") as mock_reason:
        out = node_bq_reason(
            {
                "query": q,
                "decomposition": dec,
                "supervisor_plan": sp.to_dict(),
                "task_mode": "analytical",
                "analytical_mode": True,
            }
        )
        mock_reason.assert_not_called()
    plan = out["bq_sql_plan"]
    assert plan.get("plan_source") == "class_engine"
    assert plan.get("retrieve_mode") == "planned"
    assert plan.get("bind_contracts")
    assert not plan.get("bq_sql_queries")


def test_planner_priority_analytical_escape_without_classes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Analytical reasoner runs only when no supervisor classes."""
    monkeypatch.setenv("RAG_SQL_COMPILER", "1")
    with mock.patch(
        "ml.rag.chatbot.graph.reason_bq_sql_plan",
        return_value={
            "selected_tables": ["fct_production"],
            "bq_sql_queries": ["SELECT 1"],
            "sql_source": "reasoner",
        },
    ) as mock_reason:
        out = node_bq_reason(
            {
                "query": "export analytical series",
                "decomposition": {"geography": ["Kenya"]},
                "supervisor_plan": {"classes": [], "secondary": [], "rationale": "none"},
                "task_mode": "analytical",
                "analytical_mode": True,
            }
        )
        mock_reason.assert_called_once()
    assert out["bq_sql_plan"].get("plan_source") == "analytical"


@pytest.mark.parametrize("region_label,expected_iso_count", _MULTI_REGION_PANEL)
def test_multi_region_iso3_panel(region_label: str, expected_iso_count: int) -> None:
    q = f"{region_label} agricultural activities by country 2015 to date"
    dec = expand_regions_in_decomposition({"geography": [], "time_start": "2015-01-01"}, q)
    iso = resolve_geography_iso3(
        q,
        geography=dec.get("geography"),
        expanded_regions=dec.get("expanded_regions"),
    )
    assert len(iso) == expected_iso_count


@pytest.mark.parametrize("region_label,expected_iso_count", _MULTI_REGION_PANEL)
def test_multi_region_prod_engine_planned(region_label: str, expected_iso_count: int) -> None:
    q = f"{region_label} maize production by country 2020"
    dec = expand_regions_in_decomposition(
        {"geography": [], "time_start": "2020-01-01", "time_end": "2020-12-31"},
        q,
    )
    result = ProdEngine().run_plan(q, facets=dec, card=None)
    assert result.status == "planned", result.caveats
    assert result.bind_contract is not None
    assert len(result.value_hits.get("country_iso3") or []) == expected_iso_count


@pytest.mark.parametrize("region_label", [label for label, _ in _MULTI_REGION_PANEL])
def test_multi_region_multi_class_bq_plan(region_label: str) -> None:
    q = f"{region_label} agricultural activities by country 2015 to date"
    dec = expand_regions_in_decomposition({"geography": [], "time_start": "2015-01-01"}, q)
    bundles = match_intent_bundles(q, dec)
    sp = compile_supervisor_plan(q, decomposition=dec, matched_bundles=bundles)
    assert sp.classes == ("PROD",)
    assert "FVC" in sp.secondary
    results = run_class_engines(q, supervisor_plan=sp, facets=dec)
    plan = engine_results_to_bq_plan(results)
    assert plan.get("bind_contracts")
    assert plan.get("query_intents")
    assert plan.get("plan_source") == "class_engine"
    assert not plan.get("bq_sql_queries")
    debug_tables = {row["table_id"] for row in plan.get("bq_sql_debug") or [] if row.get("table_id")}
    assert len(debug_tables) >= 2


@pytest.mark.parametrize("region_label", [label for label, _ in _MULTI_REGION_PANEL])
def test_multi_region_continental_scope_hint(region_label: str) -> None:
    q = f"{region_label} maize production by country 2020"
    dec = expand_regions_in_decomposition({"geography": []}, q)
    hint = _continental_scope_hint(q, dec.get("entities") if isinstance(dec.get("entities"), list) else None)
    assert hint is not None
    assert "do not filter country_name" in hint.lower()

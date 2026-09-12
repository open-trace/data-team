"""Tests for unified QueryIR export after decompose."""
from __future__ import annotations

from unittest import mock

import pytest

from ml.rag.chatbot.graph import node_decompose
from ml.rag.chatbot.ontology_context import sanitize_decomposition_for_bq
from ml.rag.chatbot.query_ir import (
    QueryFacets,
    QueryIR,
    compile_query_ir,
    query_ir_from_state,
)
from ml.rag.chatbot.routing_plan import RoutingPlan, build_routing_plan
from ml.rag.chatbot.class_supervisor import SupervisorPlan, compile_supervisor_plan
from ml.rag.chatbot.facet_compiler import compile_turn_contract
from ml.rag.chatbot.facet_enrich import enrich_decomposition_facets, ground_entity_roles
from ml.rag.chatbot.agri_measure_ontology import resolve_measures


def test_compile_query_ir_kenya_rice_production() -> None:
    q = "what is the production of rice in kenya in 2016"
    dec = sanitize_decomposition_for_bq(
        {
            "intent": "descriptive",
            "entities": ["rice"],
            "geography": ["Kenya"],
            "domains": ["production"],
            "time_start": "2016-01-01",
            "time_end": "2016-12-31",
            "primary_measures": ["production"],
        },
        primary_measures=["production"],
    )
    measure_hints = resolve_measures(q, dec)
    mh = measure_hints[0] if measure_hints else None
    turn = compile_turn_contract(q, dec, measure_hit=mh, task_mode_hint="fact_lookup")
    sp = compile_supervisor_plan(q, decomposition=dec, measure_hit=mh, primary_measures=["production"])
    routing = build_routing_plan(turn=turn, measure_hit=mh, supervisor_plan=sp, primary_measures=["production"])

    ir = compile_query_ir(
        query=q,
        decomposition=dec,
        turn=turn,
        routing=routing,
        supervisor=sp,
        task_mode="fact_lookup",
        recency_tier="historical_ok",
        route_candidate="full_rag",
    )

    assert ir.query == q
    assert ir.measure_id == "production"
    assert ir.primary_measures == ("production",)
    assert "rice" in ir.facets.entities
    assert ir.facets.geography == ("Kenya",)
    assert ir.turn.job in ("fact", "rank", "breakdown", "trend", "compare", "list")
    assert ir.routing.primary_measure_id == "production"
    assert ir.supervisor.classes == ("PROD",)
    assert ir.decomposition()["primary_measures"] == ["production"]


def test_query_ir_round_trip() -> None:
    q = "maize production in Ghana 2020"
    dec = {"entities": ["maize"], "geography": ["Ghana"], "primary_measures": ["production"]}
    mh = resolve_measures(q, dec)[0]
    turn = compile_turn_contract(q, dec, measure_hit=mh)
    sp = compile_supervisor_plan(q, decomposition=dec, measure_hit=mh)
    routing = build_routing_plan(turn=turn, measure_hit=mh, supervisor_plan=sp)
    ir = compile_query_ir(query=q, decomposition=dec, turn=turn, routing=routing, supervisor=sp)
    restored = QueryIR.from_dict(ir.to_dict())
    assert restored is not None
    assert restored.query == ir.query
    assert restored.facets.entities == ir.facets.entities
    assert restored.turn.measure_id == ir.turn.measure_id


def test_entity_roles_grounded_in_facets_and_ir() -> None:
    cropland_roles = ground_entity_roles("cropland in kenya", entities=["cropland"])
    assert "land_use" in cropland_roles
    assert any("cropland" in lab.lower() for lab in cropland_roles["land_use"])

    hazard_roles = ground_entity_roles("salmonella hazard", entities=["salmonella"])
    assert "hazard" in hazard_roles

    enriched = enrich_decomposition_facets(
        "cropland in kenya",
        {"entities": ["cropland"], "geography": ["Kenya"]},
    )
    assert "land_use" in (enriched.get("entity_roles") or {})

    facets = QueryFacets.from_decomposition(enriched)
    assert any(role == "land_use" for role, _ in facets.entity_roles)
    dec = facets.to_decomposition()
    assert "land_use" in dec["entity_roles"]

def test_query_ir_from_state_legacy_fields() -> None:
    q = "what is the production of rice in kenya in 2016"
    dec = sanitize_decomposition_for_bq(
        {
            "entities": ["rice"],
            "geography": ["Kenya"],
            "primary_measures": ["production"],
            "time_start": "2016-01-01",
            "time_end": "2016-12-31",
        },
        primary_measures=["production"],
    )
    mh = resolve_measures(q, dec)[0]
    turn = compile_turn_contract(q, dec, measure_hit=mh)
    sp = compile_supervisor_plan(q, decomposition=dec, measure_hit=mh)
    routing = build_routing_plan(turn=turn, measure_hit=mh, supervisor_plan=sp)
    state = {
        "query": q,
        "decomposition": dec,
        "turn_contract": turn.to_dict(),
        "routing_plan": routing.to_dict(),
        "supervisor_plan": sp.to_dict(),
        "task_mode": "fact_lookup",
    }
    ir = query_ir_from_state(state)
    assert ir is not None
    assert ir.facets.geography == ("Kenya",)
    assert ir.measure_id == "production"


def test_node_decompose_exports_query_ir() -> None:
    with mock.patch("ml.rag.chatbot.graph.decompose_query") as decompose:
        decompose.return_value = {
            "intent": "descriptive",
            "entities": ["maize", "production"],
            "geography": ["Kenya"],
            "domains": ["production"],
            "time_start": "2020-01-01",
            "time_end": "2020-12-31",
            "_decompose_llm_ms": 0.0,
            "_skipped_decompose_llm": True,
        }
        with mock.patch("ml.rag.chatbot.graph.observed_span") as span:
            span.return_value.__enter__ = mock.Mock(return_value=None)
            span.return_value.__exit__ = mock.Mock(return_value=False)
            out = node_decompose({"query": "maize production in Kenya 2020"})

    raw_ir = out.get("query_ir")
    assert isinstance(raw_ir, dict)
    ir = QueryIR.from_dict(raw_ir)
    assert ir is not None
    assert ir.query == "maize production in Kenya 2020"
    assert "maize" in ir.facets.entities
    assert ir.primary_measures == ("production",)
    assert ir.routing.indicator_classes
    assert isinstance(out.get("routing_plan"), dict)


def test_sanitize_strips_trade_noise_from_ir_facets() -> None:
    q = "maize exports from Kenya 2020"
    dec = sanitize_decomposition_for_bq(
        {
            "entities": ["maize", "exports"],
            "geography": ["Kenya"],
            "primary_measures": ["trade"],
        },
        primary_measures=["trade"],
    )
    ir = compile_query_ir(
        query=q,
        decomposition=dec,
        turn=compile_turn_contract(q, dec, measure_hit=resolve_measures(q, dec)[0]),
        routing=RoutingPlan(primary_measure_id="trade"),
        supervisor=SupervisorPlan(classes=("TRD",), secondary=(), out_of_scope=()),
    )
    assert "exports" not in ir.facets.entities
    assert "maize" in ir.facets.entities
    assert ir.primary_measures == ("trade",)


def test_node_decompose_greeting_has_early_query_ir() -> None:
    with mock.patch("ml.rag.chatbot.graph.observed_span") as span:
        span.return_value.__enter__ = mock.Mock(return_value=None)
        span.return_value.__exit__ = mock.Mock(return_value=False)
        out = node_decompose({"query": "hello"})

    ir = QueryIR.from_dict(out.get("query_ir"))
    assert ir is not None
    assert ir.route_candidate == "greeting"
    assert ir.facets.entities == ()

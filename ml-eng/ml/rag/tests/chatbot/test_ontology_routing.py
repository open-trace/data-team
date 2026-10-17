"""Adapter tests for ml.rag.chatbot.ontology_routing (flags default off)."""
from __future__ import annotations

from ml.rag.chatbot import ontology_routing as orouting


def test_plan_none_when_flag_off(monkeypatch) -> None:
    monkeypatch.delenv("ADZA_ONTOLOGY_ROUTING", raising=False)
    assert orouting.ontology_plan_for_state({"query": "cassava in Nigeria"}) is None


def test_plan_cached_on_state_when_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("ADZA_ONTOLOGY_ROUTING", "1")
    state: dict = {"query": "What is the current state of casssava in Nigeria"}
    plan = orouting.ontology_plan_for_state(state)
    assert plan is not None
    assert state["ontology_plan"] is plan
    assert state["source_basis"] == plan["source_basis"]
    again = orouting.ontology_plan_for_state(state)
    assert again is plan


def test_qdrant_payload_ids_excludes_measures_and_mv() -> None:
    plan = {
        "entities": [
            {
                "entity_id": "OT:CROP:cassava",
                "entity_type": "crop",
                "payload_ids": ["OT:CROP:cassava", "OT:GROUP:roots"],
            },
            {
                "entity_id": "OT:MEASURE:yield",
                "entity_type": "measure",
                "payload_ids": ["OT:MEASURE:yield"],
            },
            {
                "entity_id": "OT:MV:year:2020",
                "entity_type": "dimension",
                "payload_ids": ["OT:MV:year:2020"],
            },
            {"entity_id": "OT:GEO:NGA", "entity_type": "country", "payload_ids": ["OT:GEO:NGA"]},
        ]
    }
    assert orouting.qdrant_payload_ids(plan) == ["OT:CROP:cassava", "OT:GROUP:roots"]


def test_qdrant_expansion_view_only_for_qdrant_only(monkeypatch) -> None:
    monkeypatch.setenv("ADZA_ONTOLOGY_QUERY_VIEW", "1")
    plan = {
        "entities": [
            {
                "entity_id": "OT:PRODUCT:edible_fungi",
                "entity_type": "product",
                "mode": "qdrant_only",
                "qdrant_terms": ["mushroom", "edible fungi"],
            },
            {
                "entity_id": "OT:CROP:maize",
                "entity_type": "crop",
                "mode": "bq+qdrant",
                "qdrant_terms": ["maize", "corn"],
            },
        ]
    }
    view = orouting.qdrant_expansion_view(plan)
    assert view is not None
    assert "mushroom" in view
    assert "maize" not in view
    monkeypatch.delenv("ADZA_ONTOLOGY_QUERY_VIEW", raising=False)
    assert orouting.qdrant_expansion_view(plan) is None


def test_bq_value_filters_merges_without_dupes() -> None:
    plan = {
        "entities": [
            {
                "mode": "bq+qdrant",
                "bq_targets": {
                    "dim_product.product_name": ["Cassava"],
                    "fct_prices.country_iso3": ["NGA"],
                },
            },
            {
                "mode": "bq+qdrant",
                "bq_targets": {
                    "dim_product.product_name": ["Cassava", "Maize"],
                    "fct_prices.country_iso3": ["NGA"],
                },
            },
            {"mode": "qdrant_only", "bq_targets": {"dim_product.product_name": ["Mushroom"]}},
        ]
    }
    f = orouting.bq_value_filters(plan)
    assert f["dim_product.product_name"] == ["Cassava", "Maize"]
    assert f["fct_prices.country_iso3"] == ["NGA"]
    assert "Mushroom" not in f.get("dim_product.product_name", [])


def test_acf_band_ceiling_matrix() -> None:
    plan = {"acf_ceiling": "low"}
    assert orouting.acf_band_ceiling(plan, {"bigquery"}) is None
    assert orouting.acf_band_ceiling(plan, {"vector"}) == "moderate"
    assert orouting.acf_band_ceiling(plan, {"web"}) == "limited"
    assert orouting.acf_band_ceiling(plan, set()) == "low"

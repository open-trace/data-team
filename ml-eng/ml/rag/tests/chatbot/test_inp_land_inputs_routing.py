"""INP planned path prefers fct_land_inputs over legacy fertilizer/pesticide facts."""
from __future__ import annotations

from ml.rag.chatbot.class_engines.card_driven import CardDrivenEngine
from ml.rag.chatbot.class_table_router import select_table_plans
from ml.rag.chatbot.intent_bundles import match_intent_bundles
from ml.rag.chatbot.schema_card import load_schema_card
from ml.rag.chatbot.value_index import resolve_geography_iso3


_QUERY = "what is fertilizer use in kenya in 2018"
_FACETS = {
    "intent": "descriptive",
    "job": "fact",
    "geo_scope": "country",
    "geography": ["Kenya"],
    "entities": ["fertilizer"],
    "primary_measures": ["land_inputs"],
    "time_start": "2018-01-01",
    "time_end": "2018-12-31",
    "domains": ["agriculture"],
}


def test_inp_schema_card_defaults_to_land_inputs() -> None:
    card = load_schema_card("INP") or {}
    assert card.get("default_table") == "fct_land_inputs"
    tables = [str(t) for t in (card.get("tables") or [])]
    assert tables[0] == "fct_land_inputs"
    assert tables.index("fct_land_inputs") < tables.index("fct_fertilizer")
    assert tables.index("fct_land_inputs") < tables.index("fct_pesticide")
    rules = "\n".join(str(r) for r in (card.get("hard_rules") or []))
    assert "fct_land_inputs" in rules
    assert "geo_key" in rules.lower() or "Never filter geo_key" in rules


def test_inp_router_selects_fct_land_inputs() -> None:
    card = load_schema_card("INP") or {}
    bundles = match_intent_bundles(_QUERY, _FACETS)
    iso = resolve_geography_iso3(_QUERY, geography=["Kenya"])
    plans = select_table_plans(
        "INP",
        query=_QUERY,
        facets=_FACETS,
        bundles=bundles,
        card=card,
        iso_list=iso,
    )
    assert plans
    assert plans[0].table_id == "fct_land_inputs"


def test_inp_engine_planned_path_land_inputs() -> None:
    result = CardDrivenEngine("INP").run_plan(_QUERY, facets=_FACETS, card=None)
    assert result.status == "planned"
    assert result.table_id == "fct_land_inputs"
    assert result.bind_contract is not None

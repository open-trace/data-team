"""End-to-end Kenya rice planned path: decompose → engine → plan → retrieve."""
from __future__ import annotations

from unittest import mock

import pytest

from ml.rag.chatbot.class_engines.prod import ProdEngine
from ml.rag.chatbot.class_supervisor import compile_supervisor_plan
from ml.rag.chatbot.graph import node_bq_reason
from ml.rag.retrievers.bq_retriever import BQRetriever

_QUERY = "what is the production of rice in kenya in 2016"
_DECOMPOSE = {
    "intent": "descriptive",
    "job": "fact",
    "geo_scope": "country",
    "geography": ["Kenya"],
    "entities": ["rice", "production"],
    "primary_measures": ["production"],
    "time_start": "2016-01-01",
    "time_end": "2016-12-31",
    "domains": ["agriculture"],
}


def test_prod_engine_binds_rice_not_production_measure() -> None:
    result = ProdEngine().run_plan(_QUERY, facets=_DECOMPOSE, card=None)
    assert result.status == "planned"
    assert result.table_id == "agg_production_country_year"
    bind = result.bind_contract
    assert bind is not None
    nom = str(bind.get("nomenclature") or "")
    assert "Rice" in nom or "rice" in nom.lower()
    assert "production" not in (bind.get("product_literals") or [])


def test_node_bq_reason_planned_path_no_engine_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RAG_SQL_COMPILER", "0")
    sp = compile_supervisor_plan(_QUERY, decomposition=_DECOMPOSE, primary_measures=["production"])
    assert sp.classes == ("PROD",)

    with mock.patch("ml.rag.chatbot.graph.reason_bq_sql_plan") as mock_reason:
        out = node_bq_reason(
            {
                "query": _QUERY,
                "decomposition": _DECOMPOSE,
                "supervisor_plan": sp.to_dict(),
                "task_mode": "chat",
            }
        )
        mock_reason.assert_not_called()

    plan = out["bq_sql_plan"]
    assert plan.get("plan_source") == "class_engine"
    assert plan.get("retrieve_mode") == "planned"
    assert plan.get("bind_contracts")
    assert plan.get("query_intents")
    assert not plan.get("bq_sql_queries")


def test_kenya_rice_retrieve_nl2sql_only() -> None:
    good_sql = (
        "SELECT production_qty FROM `proj.mart_dev.agg_production_country_year` "
        "WHERE country_iso3 = 'KEN' AND year = 2016 LIMIT 1"
    )
    bind = {
        "agg_production_country_year": {
            "table_id": "agg_production_country_year",
            "nomenclature": (
                "TABLE: agg_production_country_year\n"
                "GEO: country_iso3 = 'KEN'\n"
                "TIME: year = 2016\n"
                "PRODUCT: product_name = 'Rice'"
            ),
            "required_filters_sql": "country_iso3 = 'KEN' AND year = 2016",
            "product_literals": ["Rice"],
            "measure_columns": ["production_qty"],
        }
    }
    retriever = BQRetriever(project_id="proj", nl2sql_enabled=True)
    client = mock.MagicMock()
    client.query.return_value.result.return_value = [{"production_qty": 100}]

    with mock.patch.object(retriever, "_nl_to_sql_queries", return_value=[good_sql]) as nl2sql_fn:
        with mock.patch.object(retriever, "_get_client", return_value=client):
            with mock.patch("ml.rag.retrievers.bq_retriever.dry_run_sql", return_value=None):
                with mock.patch("ml.rag.retrievers.bq_retriever.try_sql_template") as template_fn:
                    with mock.patch("ml.rag.retrievers.bq_retriever.try_sql_patterns") as pattern_fn:
                        items = retriever.retrieve(
                            _QUERY,
                            selected_tables=["agg_production_country_year"],
                            bind_contracts=bind,
                            query_intents=[{"goal": "fact_lookup", "pattern": "custom"}],
                            primary_measures=["production"],
                            entities=["production", "rice"],
                            task_mode="fact_lookup",
                            time_start="2016-01-01",
                            time_end="2016-12-31",
                            plan_source="class_engine",
                        )

    template_fn.assert_not_called()
    pattern_fn.assert_not_called()
    nl2sql_fn.assert_called_once()
    assert any((it.get("metadata") or {}).get("sql_source") == "nl2sql" for it in items)

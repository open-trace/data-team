"""Unit tests for synthetic BQ failure rows in the graph."""

from __future__ import annotations

from unittest.mock import patch

from ml.rag.chatbot.graph import bq_failure_debug_row, node_bq_retrieve


class _FakeRetriever:
    _last_nl2sql_raws = ["(empty LLM response)", "SELECT bad"]
    last_sql_source = "nl2sql"


def test_bq_failure_debug_row_includes_nl2sql_raw_and_source() -> None:
    row = bq_failure_debug_row(
        _FakeRetriever(),  # type: ignore[arg-type]
        status="bq_timeout",
        prep_error="BQ retrieve exceeded 15s timeout",
    )
    assert row["status"] == "bq_timeout"
    assert row["sql"] == ""
    assert "timeout" in row["prep_error"]
    assert "empty LLM" in row["nl2sql_raw"]
    assert row["sql_source"] == "nl2sql"


def test_node_bq_retrieve_keeps_no_valid_sql_debug() -> None:
    diagnostic = {
        "content": "[BQ no_valid_sql: NL2SQL produced 0 SELECT queries]",
        "source": "bigquery",
        "metadata": {
            "sql": "",
            "status": "no_valid_sql",
            "sql_source": "nl2sql",
            "prep_error": "NL2SQL produced 0 SELECT queries",
            "validation_failed": True,
        },
    }

    class FakeRetriever:
        last_sql_source = "nl2sql"
        last_bq_execute_ms = 0.0
        last_bq_nl2sql_ms = 0.0

        def retrieve(self, *_args, **_kwargs):
            return [diagnostic]

    with patch("ml.rag.chatbot.graph.BQRetriever", FakeRetriever):
        with patch("ml.rag.chatbot.bq_fact_cache.get_cached_facts", return_value=None):
            out = node_bq_retrieve(
                {
                    "query": "what is the production of rice in kenya in 2016",
                    "decomposition": {
                        "geography": ["Kenya"],
                        "primary_measures": ["production"],
                        "time_start": "2016-01-01",
                        "time_end": "2016-12-31",
                    },
                    "bq_sql_plan": {
                        "selected_tables": ["agg_production_country_year"],
                        "bind_contracts": {"agg_production_country_year": {}},
                        "plan_source": "class_engine",
                        "retrieve_mode": "planned",
                    },
                    "task_mode": "fact_lookup",
                }
            )

    assert any(d.get("status") == "no_valid_sql" for d in out["bq_sql_debug"])
    assert out["sql_source"] == "nl2sql"

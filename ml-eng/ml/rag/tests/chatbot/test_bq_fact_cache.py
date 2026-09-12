"""Tests for BQ fact session cache."""
from __future__ import annotations

from ml.rag.chatbot.bq_fact_cache import (
    bind_fingerprint,
    bq_results_from_fact_entry,
    fact_entry_from_bq_results,
    facets_subset_of_cached,
    should_reuse_facts,
)


def test_fingerprint_stable() -> None:
    a = bind_fingerprint(
        tables=["fct_production"],
        measure="production",
        geos=["Kenya"],
        time_start="2020-01-01",
        time_end="2020-12-31",
    )
    b = bind_fingerprint(
        tables=["fct_production"],
        measure="production",
        geos=["Kenya"],
        time_start="2020-01-01",
        time_end="2020-12-31",
    )
    assert a == b


def test_new_country_is_not_subset() -> None:
    assert not facets_subset_of_cached(
        {"measure": "production", "geos": ["Ghana"], "time_start": "2020-01-01", "time_end": "2020-12-31"},
        {"measure": "production", "geos": ["Kenya"], "time_start": "2020-01-01", "time_end": "2020-12-31"},
    )


def test_subset_geo_reuses() -> None:
    cached = {"measure": "production", "geos": ["Kenya", "Ghana"], "time_start": "", "time_end": ""}
    current = {"measure": "production", "geos": ["Kenya"], "time_start": "", "time_end": ""}
    assert facets_subset_of_cached(current, cached)


def test_should_reuse_with_context() -> None:
    entry = fact_entry_from_bq_results(
        [{"content": "row", "source": "bigquery", "metadata": {}}],
        fingerprint="abc",
        facets={"measure": "production", "geos": ["Kenya"]},
        query="maize Kenya",
    )
    assert entry is not None
    assert should_reuse_facts(
        context_applied=True,
        current_facets={"measure": "production", "geos": ["Kenya"]},
        cached_entry=entry,
    )
    assert bq_results_from_fact_entry(entry)[0]["content"] == "row"

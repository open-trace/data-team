"""Context isolation: short standalone statements must not merge with stale prior topic (ML-056)."""

from __future__ import annotations

from ml.rag.chatbot.query_enricher import enrich_query_with_memory


def test_unrelated_short_statement_does_not_merge_with_prior_topic() -> None:
    prior = [{"role": "user", "content": "maize production in Kasese, Uganda"}]
    out = enrich_query_with_memory("my cow is sick", recent_turns=prior)
    assert out["enriched"] is False
    assert out["enriched_query"] == "my cow is sick"


def test_treat_a_sick_cow_does_not_merge() -> None:
    prior = [{"role": "user", "content": "maize production in Kasese, Uganda"}]
    out = enrich_query_with_memory("treat a sick cow", recent_turns=prior)
    assert out["enriched"] is False


def test_i_need_help_does_not_merge() -> None:
    prior = [{"role": "user", "content": "maize production in Kasese, Uganda"}]
    out = enrich_query_with_memory("i need help", recent_turns=prior)
    assert out["enriched"] is False


def test_bare_fragment_still_merges_with_prior_topic() -> None:
    prior = [{"role": "user", "content": "maize production in Kasese, Uganda"}]
    out = enrich_query_with_memory("same crop", recent_turns=prior)
    assert out["enriched"] is True


def test_and_country_followup_still_merges() -> None:
    prior = [{"role": "user", "content": "maize production in Kasese, Uganda"}]
    out = enrich_query_with_memory("and Nigeria?", recent_turns=prior)
    assert out["enriched"] is True


def test_how_about_followup_still_merges() -> None:
    prior = [{"role": "user", "content": "maize production in Kasese, Uganda"}]
    out = enrich_query_with_memory("how about Ghana", recent_turns=prior)
    assert out["enriched"] is True


def test_bare_country_name_still_merges() -> None:
    prior = [{"role": "user", "content": "maize production in Kasese, Uganda"}]
    out = enrich_query_with_memory("kenya", recent_turns=prior)
    assert out["enriched"] is True
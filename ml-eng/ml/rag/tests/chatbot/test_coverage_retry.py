"""Tests for coverage retry gate."""
from __future__ import annotations

from ml.rag.chatbot.coverage_retry import should_coverage_retry, slots_missing_from_evidence


def test_slots_missing_when_empty() -> None:
    state = {
        "decomposition": {"geography": ["Kenya"], "entities": ["maize"]},
        "bq_results": [],
        "reranked_context": [],
    }
    assert slots_missing_from_evidence(state) is True


def test_slots_present_in_bq() -> None:
    state = {
        "decomposition": {"geography": ["Kenya"], "entities": ["maize"]},
        "bq_results": [{"content": "Kenya maize production 2020 was 4Mt"}],
        "coverage_retry": 0,
        "route_candidate": "full_rag",
    }
    assert slots_missing_from_evidence(state) is False
    assert should_coverage_retry(state) is False


def test_retry_only_once() -> None:
    state = {
        "decomposition": {"geography": ["Kenya"]},
        "bq_results": [],
        "reranked_context": [],
        "coverage_retry": 1,
        "route_candidate": "full_rag",
    }
    assert should_coverage_retry(state) is False

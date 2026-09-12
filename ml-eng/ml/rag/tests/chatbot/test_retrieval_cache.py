"""Tests for vector retrieval session KV."""
from __future__ import annotations

from ml.rag.chatbot.retrieval_cache import (
    get_cached_hits,
    retrieval_cache_key,
    set_cached_hits,
    text_hash,
)
from ml.rag.session_store import clear_fallback_for_tests


def setup_function() -> None:
    clear_fallback_for_tests()


def teardown_function() -> None:
    clear_fallback_for_tests()


def test_retrieval_cache_roundtrip() -> None:
    key = retrieval_cache_key(
        session_id="sess1",
        text="maize in Kenya",
        corpora="news_data",
        geo="Kenya",
        year="2020",
    )
    assert key is not None
    assert text_hash("maize in Kenya") in key
    hits = [
        {
            "content": "Kenya maize news",
            "score": 0.9,
            "source": "vector",
            "metadata": {"point_id": "p1", "doc_kind": "news"},
        }
    ]
    set_cached_hits(key, hits)
    cached = get_cached_hits(key)
    assert cached is not None
    assert cached[0]["metadata"]["point_id"] == "p1"
    assert "Kenya maize" in cached[0]["content"]


def test_retrieval_cache_miss_without_session() -> None:
    assert retrieval_cache_key(session_id=None, text="x", corpora="news") is None
    assert get_cached_hits(None) is None

"""Qdrant ontology_ids MatchAny filter (opentrace_ontology v3)."""
from __future__ import annotations

from ml.rag.retrievers.vector_retriever import build_qdrant_filter


def _must_keys(filt) -> set[str]:
    must = list(getattr(filt, "must", None) or [])
    keys: set[str] = set()
    for cond in must:
        key = getattr(cond, "key", None)
        if key:
            keys.add(str(key))
    return keys


def test_build_qdrant_filter_adds_ontology_ids_match_any() -> None:
    filt = build_qdrant_filter(ontology_ids=["OT:CROP:cassava", "OT:CROP:maize"])
    assert filt is not None
    assert "ontology_ids" in _must_keys(filt)
    cond = next(c for c in filt.must if getattr(c, "key", None) == "ontology_ids")
    assert list(cond.match.any) == ["OT:CROP:cassava", "OT:CROP:maize"]


def test_ontology_ids_dropped_when_not_indexed() -> None:
    filt = build_qdrant_filter(
        ontology_ids=["OT:CROP:cassava"],
        indexed_fields=frozenset({"doc_kind"}),
    )
    assert filt is None or "ontology_ids" not in _must_keys(filt)


def test_ontology_ids_capped_at_64() -> None:
    ids = [f"OT:CROP:x{i}" for i in range(80)]
    filt = build_qdrant_filter(ontology_ids=ids)
    assert filt is not None
    cond = next(c for c in filt.must if getattr(c, "key", None) == "ontology_ids")
    assert len(cond.match.any) == 64


def test_empty_ontology_ids_no_filter() -> None:
    assert build_qdrant_filter(ontology_ids=[]) is None
    assert build_qdrant_filter(ontology_ids=None) is None

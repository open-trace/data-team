"""Ingestion ontology tagging + backfill script helpers (§9 / tagging)."""
from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from ml.rag.scripts.backfill_ontology_tags import _corpus_for_collection, backfill
from ml.rag.text_processors.load_pdf_chunks_to_vector_db import _ontology_tags


def test_ontology_tags_default_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ADZA_ONTOLOGY_TAGGING", raising=False)
    assert _ontology_tags("Cassava in Nigeria", {"title": "t"}) == {}


def test_ontology_tags_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("opentrace_ontology")
    monkeypatch.setenv("ADZA_ONTOLOGY_TAGGING", "1")
    tags = _ontology_tags(
        "Cassava mosaic disease hit maize farms in Uganda.",
        {"title": "Pest update"},
    )
    assert "ontology_ids" in tags
    assert any(str(i).startswith("OT:") for i in tags["ontology_ids"])


def test_backfill_dry_run_counts_only() -> None:
    pytest.importorskip("opentrace_ontology")
    point = MagicMock()
    point.id = "p1"
    point.payload = {
        "title": "Cassava in Nigeria",
        "content": "Yields for cassava growers in Nigeria.",
    }
    client = MagicMock()
    client.scroll.side_effect = [([point], None)]
    tagged, scanned = backfill(client, "news_data", dry_run=True, batch=10)
    assert scanned == 1
    assert tagged >= 1
    client.set_payload.assert_not_called()
    assert _corpus_for_collection("news_data") == "news"
    assert os.environ.get("ADZA_ONTOLOGY_TAGGING", "0") in ("0", "false", "off", "no", "")

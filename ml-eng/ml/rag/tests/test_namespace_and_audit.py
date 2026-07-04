from __future__ import annotations

from ml.rag.source_audit import audit_priority_sources
from ml.rag.text_processors.chunk_contract import enrich_metadata


def test_enrich_metadata_carries_namespace_and_changes_ids() -> None:
    first = enrich_metadata(
        {},
        corpus="news",
        document_id="doc-1",
        chunk_index=0,
        total_chunks=1,
        text="same content",
        namespace="weather",
    )
    second = enrich_metadata(
        {},
        corpus="news",
        document_id="doc-1",
        chunk_index=0,
        total_chunks=1,
        text="same content",
        namespace="market_prices",
    )

    assert first["namespace"] == "weather"
    assert second["namespace"] == "market_prices"
    assert first["id"] != second["id"]


def test_audit_priority_sources_flags_geography_and_crop_gaps() -> None:
    report = audit_priority_sources(
        priority_sources=[
            {
                "name": "market_prices",
                "category": "market",
                "geographies": ["Kenya", "Nigeria"],
                "crops": ["maize", "sorghum"],
            }
        ],
        current_sources=["market_prices"],
    )

    assert report[0]["gaps"]["geographies"] == ["Nigeria"]
    assert report[0]["gaps"]["crops"] == ["sorghum"]

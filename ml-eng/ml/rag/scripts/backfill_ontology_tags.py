#!/usr/bin/env python3
"""Backfill ontology_ids / ontology_countries on existing Qdrant payloads (no re-embed).

Run with a ROTATED Qdrant API key — never reuse a key that was exposed in Drive docs
or chat logs. This script only prints counts; it does not dump payloads or point ids.

Example (dry-run first):
  python -m ml.rag.scripts.backfill_ontology_tags --collection news_data --dry-run
  python -m ml.rag.scripts.backfill_ontology_tags --collection news_data --batch 256
"""
from __future__ import annotations

import argparse
import os
from typing import Any

from ml.rag.retrievers.vector_retriever import make_qdrant_client
from ml.rag.scripts.qdrant_collection_specs import PAYLOAD_INDEXES, ensure_payload_indexes


def _corpus_for_collection(collection: str) -> str:
    news = os.environ.get("QDRANT_COLLECTION_NEWS", "news_data")
    academic = os.environ.get("QDRANT_COLLECTION_ACADEMIC_PAPERS", "academic_papers")
    policies = os.environ.get("QDRANT_COLLECTION_POLICIES", "policies")
    reports = os.environ.get("QDRANT_COLLECTION_PUBLIC_REPORTS", "public_reports")
    ota = os.environ.get("QDRANT_COLLECTION_OTA_INSIGHTS", "OTA_insights")
    mapping = {
        news: "news",
        academic: "research",
        policies: "research",
        reports: "research",
        ota: "ota",
    }
    if collection in mapping:
        return mapping[collection]
    if collection in PAYLOAD_INDEXES:
        return collection
    return "news"


def backfill(
    client: Any,
    collection: str,
    *,
    dry_run: bool = False,
    batch: int = 256,
) -> tuple[int, int]:
    """Scroll → tag_chunk → set_payload. Returns (tagged_count, scanned_count)."""
    from opentrace_ontology import tag_chunk

    corpus = _corpus_for_collection(collection)
    ensure_payload_indexes(client, collection, corpus)
    offset: Any = None
    tagged = 0
    scanned = 0
    while True:
        points, offset = client.scroll(
            collection_name=collection,
            limit=max(1, batch),
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )
        if not points:
            break
        for point in points:
            scanned += 1
            pl = point.payload or {}
            text = f"{pl.get('title') or ''}\n{pl.get('content') or ''}"[:6000]
            tags = tag_chunk(text)
            tags = {k: v for k, v in tags.items() if v}
            if not tags:
                continue
            tagged += 1
            if dry_run:
                continue
            client.set_payload(
                collection_name=collection,
                payload=tags,
                points=[point.id],
            )
        if offset is None:
            break
    return tagged, scanned


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill Qdrant ontology payload tags (rotated API key required)"
    )
    parser.add_argument("--collection", required=True, help="Qdrant collection name")
    parser.add_argument("--dry-run", action="store_true", help="Count only; no set_payload")
    parser.add_argument("--batch", type=int, default=256, help="Scroll page size")
    args = parser.parse_args()

    client = make_qdrant_client()
    tagged, scanned = backfill(
        client,
        args.collection,
        dry_run=args.dry_run,
        batch=args.batch,
    )
    mode = "would_tag" if args.dry_run else "tagged"
    print(f"{args.collection}: scanned={scanned} {mode}={tagged}")


if __name__ == "__main__":
    main()

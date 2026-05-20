"""
Load OTA analytics insight JSONL into the ``OTA_insights`` Qdrant collection (triple vectors).

JSONL rows support:
  - id (str), optional text (str)
  - insight_text / text_insight, metric_text / text_metric, recommendation_text / text_recommendation
    at top level or inside metadata
  - metadata (dict): merged; ``info_type`` defaults to ``ota_insight``

Domains are inferred via shared agrifood taxonomy when absent.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from ml.rag.text_processors.load_pdf_chunks_to_vector_db import upsert_jsonl_to_qdrant_for_collection


from ml.rag.paths import preprocessed_jsonl_for_corpus

DEFAULT_INPUT = preprocessed_jsonl_for_corpus("ota")


def load_ota_insights_to_qdrant(
    *,
    input_path: Path,
    collection: str,
    reset: bool,
    batch_size: int,
) -> int:
    return upsert_jsonl_to_qdrant_for_collection(
        input_path=input_path,
        collection=collection,
        reset=reset,
        batch_size=batch_size,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Load OTA insight JSONL into Qdrant.")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT, help=f"Path to JSONL (default: {DEFAULT_INPUT})")
    p.add_argument(
        "--collection",
        type=str,
        default=os.environ.get("QDRANT_COLLECTION_OTA_INSIGHTS", "OTA_insights"),
        help="Qdrant collection name for OTA insights.",
    )
    p.add_argument("--batch-size", type=int, default=200, help="Upsert batch size (default: 200)")
    p.add_argument("--reset", action="store_true", help="Delete and recreate the collection before loading.")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    inserted = load_ota_insights_to_qdrant(
        input_path=args.input,
        collection=args.collection,
        reset=bool(args.reset),
        batch_size=int(args.batch_size),
    )
    print(f"Upserted {inserted} rows into OTA_insights collection '{args.collection}' in Qdrant")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
Load preprocessed data_descriptions chunk JSONL into the DATA_DESCRIPTIONS Qdrant collection.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from ml.rag.text_processors.load_pdf_chunks_to_vector_db import PAYLOAD_BQ_DESCRIPTIONS, upsert_jsonl_to_qdrant_for_collection


from ml.rag.paths import preprocessed_jsonl_for_corpus

DEFAULT_INPUT = preprocessed_jsonl_for_corpus("data_description")

def load_data_descriptions_to_qdrant(
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
        allowed_payload_keys=PAYLOAD_BQ_DESCRIPTIONS,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Load DATA_DESCRIPTIONS chunk JSONL into Qdrant.")
    p.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=(
            f"Path to chunk JSONL (default: {DEFAULT_INPUT}, under data/local/preprocessed_data/). "
        ),
    )
    p.add_argument(
        "--collection",
        type=str,
        default=os.environ.get("QDRANT_COLLECTION_DATA_DESCRIPTIONS", "BQ_table_descriptions"),
        help="Qdrant collection name for data descriptions.",
    )
    p.add_argument("--batch-size", type=int, default=200, help="Upsert batch size (default: 200)")
    p.add_argument("--reset", action="store_true", help="Delete and recreate the collection before loading.")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    inserted = load_data_descriptions_to_qdrant(
        input_path=args.input,
        collection=args.collection,
        reset=bool(args.reset),
        batch_size=int(args.batch_size),
    )
    print(f"Upserted {inserted} chunks into DATA_DESCRIPTIONS collection '{args.collection}' in Qdrant")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


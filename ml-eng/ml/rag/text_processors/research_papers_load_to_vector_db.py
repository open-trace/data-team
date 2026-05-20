"""
Load preprocessed research_papers chunk JSONL into the RESEARCH_PAPERS Qdrant collection.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from ml.rag.text_processors.load_pdf_chunks_to_vector_db import PAYLOAD_RESEARCH, upsert_jsonl_to_qdrant_for_collection


from ml.rag.paths import preprocessed_jsonl_for_corpus

DEFAULT_INPUT = preprocessed_jsonl_for_corpus("research")

def load_research_papers_to_qdrant(
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
        allowed_payload_keys=PAYLOAD_RESEARCH,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Load RESEARCH_PAPERS chunk JSONL into Qdrant.")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT, help=f"Path to chunk JSONL (default: {DEFAULT_INPUT})")
    p.add_argument(
        "--collection",
        type=str,
        default=os.environ.get("QDRANT_COLLECTION_RESEARCH_PAPERS", "research_other_papers"),
        help="Qdrant collection name for research papers.",
    )
    p.add_argument("--batch-size", type=int, default=200, help="Upsert batch size (default: 200)")
    p.add_argument("--reset", action="store_true", help="Delete and recreate the collection before loading.")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    inserted = load_research_papers_to_qdrant(
        input_path=args.input,
        collection=args.collection,
        reset=bool(args.reset),
        batch_size=int(args.batch_size),
    )
    print(f"Upserted {inserted} chunks into RESEARCH_PAPERS collection '{args.collection}' in Qdrant")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


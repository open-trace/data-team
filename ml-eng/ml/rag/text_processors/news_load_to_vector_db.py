"""
Load preprocessed news chunk JSONL into the NEWS Qdrant collection.

This is intentionally separate from other corpus loaders so each collection has
its own script and can evolve independently.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from ml.rag.text_processors.load_pdf_chunks_to_vector_db import upsert_jsonl_to_qdrant_dual


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = REPO_ROOT / "data" / "local" / "news_chunks.jsonl"

def load_news_to_qdrant(
    *,
    input_path: Path,
    collection: str,
    reset: bool,
    batch_size: int,
) -> int:
    return upsert_jsonl_to_qdrant_dual(
        input_path=input_path,
        collection=collection,
        reset=reset,
        batch_size=batch_size,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Load NEWS chunk JSONL into Qdrant news collection.")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT, help=f"Path to chunk JSONL (default: {DEFAULT_INPUT})")
    p.add_argument(
        "--collection",
        type=str,
        default=os.environ.get("QDRANT_COLLECTION_NEWS", "opentrace_news"),
        help="Qdrant collection name for news.",
    )
    p.add_argument("--batch-size", type=int, default=200, help="Upsert batch size (default: 200)")
    p.add_argument("--reset", action="store_true", help="Delete and recreate the collection before loading.")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    inserted = load_news_to_qdrant(
        input_path=args.input,
        collection=args.collection,
        reset=bool(args.reset),
        batch_size=int(args.batch_size),
    )
    print(f"Upserted {inserted} chunks into NEWS collection '{args.collection}' in Qdrant")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


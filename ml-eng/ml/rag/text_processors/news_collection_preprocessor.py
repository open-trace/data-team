"""
Preprocess News corpus (.txt with YAML front matter) into chunk JSONL for vector embedding.

Thin wrapper around `news_preprocessor.py` so the news collection has its own script.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from ml.rag.text_processors.news_preprocessor import preprocess_folder, write_jsonl


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_PATH = REPO_ROOT / "data" / "local" / "ingestion_chunks" / "news_chunks.jsonl"

def preprocess_news_collection(
    *,
    input_dir: Path,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    chunk_chars: int = 1200,
    overlap_chars: int = 200,
    max_files: int | None = None,
) -> int:
    rows = preprocess_folder(
        input_dir=input_dir,
        chunk_chars=max(200, int(chunk_chars)),
        overlap_chars=max(0, int(overlap_chars)),
        max_files=max_files,
    )
    return write_jsonl(rows, output_path)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Preprocess news .txt files into JSONL chunks.")
    p.add_argument("--input-dir", type=Path, required=True, help="Root directory for saved news .txt articles.")
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help=f"Output JSONL path (default: {DEFAULT_OUTPUT_PATH})")
    p.add_argument("--chunk-chars", type=int, default=1200, help="Chunk size in chars.")
    p.add_argument("--overlap", type=int, default=200, help="Chunk overlap in chars.")
    p.add_argument("--max-files", type=int, default=None, help="Optional cap on number of files processed.")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    input_dir: Path = args.input_dir
    output_path: Path = args.output
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist or is not a directory: {input_dir}")

    n = preprocess_news_collection(
        input_dir=input_dir,
        output_path=output_path,
        chunk_chars=args.chunk_chars,
        overlap_chars=args.overlap,
        max_files=args.max_files,
    )
    print(f"Wrote {n} chunks to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


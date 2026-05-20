"""
Preprocess BQ table description DOCX files into chunk JSONL.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from ml.rag.paths import preprocessed_jsonl_for_corpus
from ml.rag.text_processors.preprocess.engines.bq import preprocess_folder
from ml.rag.text_processors.preprocess.write_jsonl import write_chunks_jsonl

DEFAULT_OUTPUT_PATH = preprocessed_jsonl_for_corpus("data_description")


def preprocess_data_descriptions(
    *,
    input_dir: Path,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    chunk_chars: int = 1000,
    overlap_chars: int = 120,
) -> int:
    _ = chunk_chars, overlap_chars
    chunks = preprocess_folder(input_dir)
    return write_chunks_jsonl(chunks, output_path)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Preprocess data description DOCX files into JSONL chunks.")
    p.add_argument("--input-dir", type=Path, required=True)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    if not args.input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {args.input_dir}")
    n = preprocess_data_descriptions(input_dir=args.input_dir, output_path=args.output)
    print(f"Wrote {n} chunks to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

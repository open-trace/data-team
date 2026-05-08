"""
Preprocess dataset / table descriptions (DOCX) into chunk JSONL for vector embedding.

This is a thin wrapper around `bq_description_preprocessor.py` so the data-descriptions
collection has its own preprocessor script.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from ml.rag.text_processors.bq_description_preprocessor import preprocess_folder, write_chunks_jsonl


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_PATH = REPO_ROOT / "data" / "local" / "ingestion_chunks" / "data_descriptions_chunks.jsonl"

def preprocess_data_descriptions(
    *,
    input_dir: Path,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    chunk_chars: int = 1000,
    overlap_chars: int = 120,
) -> int:
    records = preprocess_folder(
        input_dir=input_dir,
        chunk_chars=max(200, int(chunk_chars)),
        overlap_chars=max(0, int(overlap_chars)),
    )
    return write_chunks_jsonl(records, output_path)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Preprocess data description DOCX files into JSONL chunks.")
    p.add_argument("--input-dir", type=Path, required=True, help="Folder containing DOCX files (recursive).")
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help=f"Output JSONL path (default: {DEFAULT_OUTPUT_PATH})")
    p.add_argument("--chunk-chars", type=int, default=1000, help="Chunk size in chars.")
    p.add_argument("--overlap", type=int, default=120, help="Chunk overlap in chars.")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    input_dir: Path = args.input_dir
    output_path: Path = args.output
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist or is not a directory: {input_dir}")
    n = preprocess_data_descriptions(
        input_dir=input_dir,
        output_path=output_path,
        chunk_chars=args.chunk_chars,
        overlap_chars=args.overlap,
    )
    print(f"Wrote {n} chunks to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


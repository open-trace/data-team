"""
Preprocess Research + official papers (PDFs) into chunk JSONL for vector embedding.

This is a thin wrapper around `pdf_preprocessor.py` with defaults aligned to the
Google Drive → Qdrant ingestion pipeline.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from ml.rag.text_processors.pdf_preprocessor import preprocess_pdf_folder, write_chunks_jsonl


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = REPO_ROOT / "data" / "local" / "gdrive_cache"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "data" / "local" / "ingestion_chunks" / "research_chunks.jsonl"

def preprocess_research_papers(
    *,
    input_dir: Path,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> int:
    records = preprocess_pdf_folder(input_dir=input_dir)
    return write_chunks_jsonl(records, output_path)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Preprocess research PDFs into JSONL chunks.")
    p.add_argument("--input-dir", type=Path, required=True, help="Folder containing PDFs (recursive).")
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help=f"Output JSONL path (default: {DEFAULT_OUTPUT_PATH})")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    input_dir: Path = args.input_dir
    output_path: Path = args.output
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist or is not a directory: {input_dir}")
    n = preprocess_research_papers(input_dir=input_dir, output_path=output_path)
    print(f"Wrote {n} chunks to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


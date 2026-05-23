"""
Preprocess research / policy PDFs into chunk JSONL for vector embedding.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from ml.rag.paths import preprocessed_jsonl_for_corpus
from ml.rag.text_processors.preprocess.engines.research import preprocess_folder as _preprocess_folder
from ml.rag.text_processors.preprocess.write_jsonl import append_chunks_jsonl, write_chunks_jsonl

DEFAULT_OUTPUT_PATH = preprocessed_jsonl_for_corpus("research")


def preprocess_research_papers(
    *,
    input_dir: Path,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    doc_kind: str = "academic_article",
    append: bool = False,
) -> int:
    chunks = _preprocess_folder(input_dir, doc_kind=doc_kind)
    if append:
        return append_chunks_jsonl(chunks, output_path)
    return write_chunks_jsonl(chunks, output_path)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Preprocess research PDFs into JSONL chunks.")
    p.add_argument("--input-dir", type=Path, required=True)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    p.add_argument(
        "--doc-kind",
        type=str,
        default="academic_article",
        choices=("academic_article", "policy_document", "public_report"),
    )
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    if not args.input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {args.input_dir}")
    n = preprocess_research_papers(
        input_dir=args.input_dir,
        output_path=args.output,
        doc_kind=args.doc_kind,
    )
    print(f"Wrote {n} chunks to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

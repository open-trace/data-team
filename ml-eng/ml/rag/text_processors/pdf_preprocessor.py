"""
PDF preprocessing — delegates to ``preprocess.engines.research``.

Legacy CLI retained for direct use:
  PYTHONPATH=. python -m ml.rag.text_processors.pdf_preprocessor --input-dir ...
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path

from ml.rag.paths import ML_ENG_ROOT, preprocessed_jsonl_for_corpus
from ml.rag.text_processors.preprocess.engines.research import preprocess_folder, preprocess_pdf
from ml.rag.text_processors.preprocess.models import ChunkOutput
from ml.rag.text_processors.preprocess.write_jsonl import write_chunks_jsonl

DEFAULT_INPUT_DIR = ML_ENG_ROOT / "ml" / "rag" / "data" / "Text_Documents"
DEFAULT_OUTPUT_PATH = preprocessed_jsonl_for_corpus("research")


@dataclass
class ChunkRecord:
    """Backward-compatible record shape (loader accepts JSONL metadata)."""

    id: str
    source_file: str
    chunk_type: str
    text: str
    metadata: dict


def _to_chunk_record(ch: ChunkOutput, source_file: str) -> ChunkRecord:
    meta = dict(ch.metadata)
    return ChunkRecord(
        id=ch.id,
        source_file=meta.get("source_file", source_file),
        chunk_type="structure_token",
        text=ch.text,
        metadata=meta,
    )


def preprocess_pdf_folder(input_dir: Path, *, doc_kind: str = "academic_article") -> list[ChunkRecord]:
    return [_to_chunk_record(c, str(c.metadata.get("source_file", ""))) for c in preprocess_folder(input_dir, doc_kind=doc_kind)]


def write_chunks_jsonl_records(records, output_path: Path) -> int:
    import json

    output_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with output_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")
            n += 1
    return n


def main() -> int:
    p = argparse.ArgumentParser(description="Preprocess PDFs into JSONL chunks.")
    p.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    p.add_argument("--doc-kind", type=str, default="academic_article")
    args = p.parse_args()
    if not args.input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {args.input_dir}")
    chunks = preprocess_folder(args.input_dir, doc_kind=args.doc_kind)
    n = write_chunks_jsonl(chunks, args.output)
    print(f"Wrote {n} chunks to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
Preprocess BigQuery table description DOCX files into JSONL chunks for vector embedding.

Input folder (default):
    ml/rag/BQ data description

Output JSONL rows:
    - id
    - source_file
    - chunk_type
    - text
    - metadata

Important metadata:
    - type: "BQ <table_name> description"
    - table_name: <table_name>

Usage:
    PYTHONPATH=. python -m ml.rag.bq_description_preprocessor
    PYTHONPATH=. python -m ml.rag.bq_description_preprocessor --output data/local/bq_description_chunks.jsonl
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET


@dataclass
class ChunkRecord:
    id: str
    source_file: str
    chunk_type: str
    text: str
    metadata: dict[str, str | int | float | bool]


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = REPO_ROOT / "ml" / "rag" / "BQ data description"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "data" / "local" / "bq_description_chunks.jsonl"


def _table_name_from_path(docx_path: Path) -> str:
    stem = docx_path.stem.strip()
    stem = re.sub(r"\s+", " ", stem)
    # Keep table names readable while removing dangerous punctuation noise.
    stem = re.sub(r"[^A-Za-z0-9_ ]+", "", stem).strip()
    return stem.replace(" ", "_") or "unknown_table"


def list_docx_files(input_dir: Path) -> list[Path]:
    return sorted([p for p in input_dir.rglob("*.docx") if p.is_file()], key=lambda p: str(p).lower())


def read_docx_text(docx_path: Path) -> str:
    """
    Read DOCX text by parsing word/document.xml (no extra dependency required).
    """
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraphs: list[str] = []

    with zipfile.ZipFile(docx_path) as zf:
        xml_bytes = zf.read("word/document.xml")

    root = ET.fromstring(xml_bytes)
    for para in root.findall(".//w:p", ns):
        runs = para.findall(".//w:t", ns)
        text = "".join((r.text or "") for r in runs).strip()
        if text:
            paragraphs.append(text)

    return "\n\n".join(paragraphs).strip()


def normalize_text(raw_text: str) -> str:
    text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _chunk_id(source_file: str, chunk_type: str, index: int, text: str) -> str:
    digest = hashlib.sha1(f"{source_file}|{chunk_type}|{index}|{text[:180]}".encode("utf-8")).hexdigest()
    return f"{chunk_type}_{digest}"


def build_chunks(text: str, chunk_chars: int = 1000, overlap_chars: int = 120) -> list[str]:
    chunks: list[str] = []
    n = len(text)
    start = 0
    while start < n:
        end = min(start + chunk_chars, n)
        window = text[start:end]
        boundary = max(window.rfind("\n\n"), window.rfind(". "), window.rfind("; "))
        if boundary > int(chunk_chars * 0.6):
            end = start + boundary + 1
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= n:
            break
        start = max(0, end - overlap_chars)
    return chunks


def preprocess_docx(docx_path: Path, chunk_chars: int = 1000, overlap_chars: int = 120) -> list[ChunkRecord]:
    source_file = str(docx_path)
    table_name = _table_name_from_path(docx_path)
    text = normalize_text(read_docx_text(docx_path))
    if not text:
        return []

    chunks = build_chunks(text=text, chunk_chars=chunk_chars, overlap_chars=overlap_chars)
    total = len(chunks)
    records: list[ChunkRecord] = []
    for i, chunk in enumerate(chunks):
        records.append(
            ChunkRecord(
                id=_chunk_id(source_file, "bq_description", i, chunk),
                source_file=source_file,
                chunk_type="bq_description",
                text=chunk,
                metadata={
                    "type": f"BQ {table_name} description",
                    "table_name": table_name,
                    "source_kind": "bq_table_description_docx",
                    "chunk_index": i,
                    "total_chunks": total,
                },
            )
        )
    return records


def preprocess_folder(input_dir: Path, chunk_chars: int = 1000, overlap_chars: int = 120) -> list[ChunkRecord]:
    records: list[ChunkRecord] = []
    for docx_file in list_docx_files(input_dir):
        records.extend(preprocess_docx(docx_file, chunk_chars=chunk_chars, overlap_chars=overlap_chars))
    return records


def write_chunks_jsonl(records: Iterable[ChunkRecord], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")
            count += 1
    return count


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Preprocess BQ description DOCX files into JSONL chunks."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Folder containing DOCX files (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output JSONL path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument("--chunk-chars", type=int, default=1000, help="Chunk size in chars.")
    parser.add_argument("--overlap", type=int, default=120, help="Chunk overlap in chars.")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    input_dir: Path = args.input_dir
    output_path: Path = args.output
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist or is not a directory: {input_dir}")

    records = preprocess_folder(
        input_dir=input_dir,
        chunk_chars=max(200, int(args.chunk_chars)),
        overlap_chars=max(0, int(args.overlap)),
    )
    count = write_chunks_jsonl(records, output_path)
    print(f"Wrote {count} chunks to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
BQ description DOCX helpers (legacy import path).

Chunking is implemented in ``preprocess.engines.bq``; use ``data_descriptions_preprocessor`` CLI.
"""
from __future__ import annotations

from pathlib import Path

from ml.rag.text_processors.preprocess.docx_io import read_docx_text
from ml.rag.text_processors.preprocess.engines.bq import preprocess_docx, preprocess_folder

__all__ = ["read_docx_text", "preprocess_docx", "preprocess_folder", "list_docx_files"]


def list_docx_files(input_dir: Path) -> list[Path]:
    return sorted([p for p in input_dir.rglob("*.docx") if p.is_file()], key=lambda p: str(p).lower())

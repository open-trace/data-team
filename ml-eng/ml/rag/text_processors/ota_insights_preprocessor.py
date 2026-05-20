"""
Consolidate and chunk OTA insight JSON/JSONL from a folder into one JSONL file.
"""

from __future__ import annotations

from pathlib import Path

from ml.rag.paths import preprocessed_jsonl_for_corpus
from ml.rag.text_processors.preprocess.engines.ota import preprocess_folder
from ml.rag.text_processors.preprocess.write_jsonl import write_chunks_jsonl

DEFAULT_OUTPUT_PATH = preprocessed_jsonl_for_corpus("ota")


def consolidate_ota_staging(
    *,
    input_dir: Path,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> int:
    chunks = preprocess_folder(input_dir)
    return write_chunks_jsonl(chunks, output_path)

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from ml.rag.text_processors.preprocess.models import ChunkOutput


def write_chunks_jsonl(chunks: Iterable[ChunkOutput], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with output_path.open("w", encoding="utf-8") as f:
        for ch in chunks:
            f.write(json.dumps(ch.to_jsonl_dict(), ensure_ascii=False) + "\n")
            n += 1
    return n


def append_chunks_jsonl(chunks: Iterable[ChunkOutput], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with output_path.open("a", encoding="utf-8") as f:
        for ch in chunks:
            f.write(json.dumps(ch.to_jsonl_dict(), ensure_ascii=False) + "\n")
            n += 1
    return n

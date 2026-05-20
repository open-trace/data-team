"""
News preprocessing — delegates to ``preprocess.engines.news``.

Kept for imports of ``ChunkRow``, ``preprocess_folder``, ``write_jsonl``.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from ml.rag.paths import ML_ENG_ROOT, preprocessed_jsonl_for_corpus
from ml.rag.text_processors.preprocess.engines import news as _news
from ml.rag.text_processors.preprocess.models import ChunkOutput

DEFAULT_INPUT_DIR = ML_ENG_ROOT / "data" / "local" / "web_news_rss"
DEFAULT_OUTPUT_PATH = preprocessed_jsonl_for_corpus("news")


@dataclass
class ChunkRow:
    id: str
    text: str
    metadata: dict


def _to_row(ch: ChunkOutput) -> ChunkRow:
    return ChunkRow(id=ch.id, text=ch.text, metadata=dict(ch.metadata))


def preprocess_folder(*args, **kwargs) -> list[ChunkRow]:
    return [_to_row(c) for c in _news.preprocess_folder(*args, **kwargs)]


def preprocess_news_document(*args, **kwargs) -> list[ChunkRow]:
    return [_to_row(c) for c in _news.preprocess_document(*args, **kwargs)]


def write_jsonl(rows: Iterable[ChunkRow], output_path: Path) -> int:
    import json

    output_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with output_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(asdict(row), ensure_ascii=False) + "\n")
            n += 1
    return n


normalize_published_at = _news.normalize_published_at
list_news_document_files = _news.list_news_document_files

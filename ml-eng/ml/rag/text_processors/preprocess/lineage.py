from __future__ import annotations

from ml.rag.text_processors.chunk_contract import make_chunk_id
from ml.rag.text_processors.chunking_config import CorpusKey


def section_parent_id(
    *,
    corpus: CorpusKey,
    document_id: str,
    hierarchy_path: str,
) -> str:
    """Stable id for a section parent (metadata anchor for child chunks)."""
    return make_chunk_id(
        corpus=corpus,
        document_id=document_id,
        chunk_index=-1,
        text=f"section::{hierarchy_path}",
    )


def semantic_lane_for_path(hierarchy_path: str) -> str:
    hp = (hierarchy_path or "").lower()
    if hp == "abstract" or hp.startswith("abstract/"):
        return "abstract"
    return "content"

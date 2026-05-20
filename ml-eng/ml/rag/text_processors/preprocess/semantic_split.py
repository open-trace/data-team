"""
Embedding-based semantic chunking using the same model family as ingest (E5 via sentence-transformers).

Breakpoints are placed where cosine similarity between adjacent sentences drops
(below a percentile threshold), then groups are merged to respect target_tokens.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import numpy as np

from ml.rag.text_processors.chunking_config import ChunkingProfile
from ml.rag.text_processors.preprocess.llama_split import (
    _enforce_max_tokens,
    _split_sentences,
    _split_sentences_token_aware,
)
from ml.rag.text_processors.preprocess.tokens import count_tokens

logger = logging.getLogger(__name__)

_MODEL_CACHE: dict[str, Any] = {}
_EMBEDDINGS_USABLE: bool | None = None
_WARNED_UNAVAILABLE = False


def semantic_chunking_enabled() -> bool:
    return os.environ.get("RAG_SEMANTIC_CHUNKING", "1").strip().lower() not in ("0", "false", "no")


def _breakpoint_percentile() -> float:
    raw = os.environ.get("RAG_SEMANTIC_BREAKPOINT_PERCENTILE", "95").strip()
    try:
        return float(raw)
    except ValueError:
        return 95.0


def _warn_unavailable(msg: str) -> None:
    global _WARNED_UNAVAILABLE
    if not _WARNED_UNAVAILABLE:
        logger.warning("%s — using sentence/token chunking instead.", msg)
        _WARNED_UNAVAILABLE = True


def _embeddings_usable() -> bool:
    """Probe once whether sentence-transformers + torch can load in this venv."""
    global _EMBEDDINGS_USABLE
    if _EMBEDDINGS_USABLE is not None:
        return _EMBEDDINGS_USABLE
    if not semantic_chunking_enabled():
        _EMBEDDINGS_USABLE = False
        return False
    try:
        major = int(str(np.__version__).split(".")[0])
        if major >= 2:
            _warn_unavailable(
                f"NumPy {np.__version__} is incompatible with this torch build; "
                "pip install 'numpy>=1.24,<2' in ml-eng venv"
            )
            _EMBEDDINGS_USABLE = False
            return False
        from sentence_transformers import SentenceTransformer  # type: ignore[import-not-found]  # noqa: F401

        _EMBEDDINGS_USABLE = True
    except Exception as exc:
        _warn_unavailable(f"sentence-transformers unavailable ({exc})")
        _EMBEDDINGS_USABLE = False
    return _EMBEDDINGS_USABLE


def _token_fallback(text: str, profile: ChunkingProfile) -> list[str]:
    return _enforce_max_tokens(_split_sentences_token_aware(text, profile), profile)


def _get_embedding_model(model_id: str) -> Any:
    if model_id not in _MODEL_CACHE:
        from sentence_transformers import SentenceTransformer  # type: ignore[import-not-found]

        _MODEL_CACHE[model_id] = SentenceTransformer(model_id)
    return _MODEL_CACHE[model_id]


def _embed_sentences(sentences: list[str], model_id: str) -> np.ndarray:
    model = _get_embedding_model(model_id)
    vecs = model.encode(
        sentences,
        batch_size=min(32, max(1, len(sentences))),
        show_progress_bar=False,
        normalize_embeddings=True,
    )
    return np.asarray(vecs, dtype=np.float32)


def _group_sentences_at_breakpoints(sentences: list[str], breakpoints: set[int]) -> list[str]:
    if not sentences:
        return []
    groups: list[list[str]] = []
    current: list[str] = [sentences[0]]
    for i in range(len(sentences) - 1):
        if i in breakpoints:
            groups.append(current)
            current = [sentences[i + 1]]
        else:
            current.append(sentences[i + 1])
    groups.append(current)
    return [" ".join(g).strip() for g in groups if g]


def semantic_split_text(text: str, profile: ChunkingProfile) -> list[str]:
    """Split text at semantic boundaries, then enforce max token size per chunk."""
    text = text.strip()
    if not text or not _embeddings_usable():
        return _token_fallback(text, profile)

    target = profile.target_tokens
    model_id = profile.embedding_model

    if count_tokens(text, model_id=model_id) <= target:
        return [text]

    sentences = _split_sentences(text)
    if len(sentences) <= 1:
        return _enforce_max_tokens([text], profile)

    expanded: list[str] = []
    for sent in sentences:
        if count_tokens(sent, model_id=model_id) > target:
            expanded.extend(_split_sentences_token_aware(sent, profile))
        else:
            expanded.append(sent)
    sentences = [s for s in expanded if s.strip()]
    if len(sentences) <= 1:
        return _enforce_max_tokens([text], profile)

    try:
        embeddings = _embed_sentences(sentences, model_id)
        sims: list[float] = []
        for i in range(len(sentences) - 1):
            sims.append(float(np.dot(embeddings[i], embeddings[i + 1])))

        if not sims:
            return _enforce_max_tokens([" ".join(sentences)], profile)

        threshold = float(np.percentile(sims, _breakpoint_percentile()))
        breakpoints = {i for i, s in enumerate(sims) if s <= threshold}
        groups = _group_sentences_at_breakpoints(sentences, breakpoints)
    except Exception as exc:
        _warn_unavailable(f"semantic embed failed ({exc})")
        return _token_fallback(text, profile)

    merged: list[str] = []
    buf = ""
    buf_tok = 0
    for g in groups:
        gt = count_tokens(g, model_id=model_id)
        if not buf:
            buf, buf_tok = g, gt
            continue
        if buf_tok + gt <= target:
            buf = f"{buf}\n\n{g}".strip()
            buf_tok += gt
        else:
            merged.append(buf)
            buf, buf_tok = g, gt
    if buf:
        merged.append(buf)

    return _enforce_max_tokens(merged, profile)

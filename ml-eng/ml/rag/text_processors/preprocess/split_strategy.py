"""
Per-corpus chunking strategies (see chunking_config.ChunkingStrategy).
"""
from __future__ import annotations

import re

from ml.rag.text_processors.chunking_config import ChunkingProfile, ChunkingStrategy
from ml.rag.text_processors.preprocess.llama_split import (
    TextSlice,
    _enforce_max_tokens,
    _split_sentences_token_aware,
    cap_slices,
)
from ml.rag.text_processors.preprocess.semantic_split import semantic_chunking_enabled, semantic_split_text
from ml.rag.text_processors.preprocess.tokens import count_tokens


def _paragraphs(text: str) -> list[str]:
    return [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]


def _recursive_semantic_split(text: str, profile: ChunkingProfile) -> list[str]:
    """news_data: paragraph recursion, semantic fallback when a block exceeds target."""
    text = text.strip()
    if not text:
        return []

    target = profile.target_tokens
    model_id = profile.embedding_model
    pieces: list[str] = []

    for para in _paragraphs(text):
        pt = count_tokens(para, model_id=model_id)
        if pt <= target:
            pieces.append(para)
        elif semantic_chunking_enabled():
            pieces.extend(semantic_split_text(para, profile))
        else:
            pieces.extend(_split_sentences_token_aware(para, profile))

    return _enforce_max_tokens(pieces, profile)


def _hierarchical_semantic_split(text: str, profile: ChunkingProfile) -> list[str]:
    """research: semantic boundaries within each section block."""
    if semantic_chunking_enabled():
        return semantic_split_text(text, profile)
    return _enforce_max_tokens(_split_sentences_token_aware(text, profile), profile)


def _lane_semantic_split(text: str, profile: ChunkingProfile) -> list[str]:
    """OTA: semantic chunking within a single lane's text."""
    return semantic_split_text(text, profile)


def _schema_only_split(text: str, profile: ChunkingProfile) -> list[str]:
    """BQ: schema/table blocks — sentence + token cap only."""
    return _enforce_max_tokens(_split_sentences_token_aware(text, profile), profile)


def split_by_strategy(text: str, profile: ChunkingProfile) -> list[str]:
    strategy: ChunkingStrategy = profile.chunking_strategy
    if strategy == "recursive_semantic":
        return _recursive_semantic_split(text, profile)
    if strategy == "hierarchical_semantic":
        return _hierarchical_semantic_split(text, profile)
    if strategy == "lane_semantic":
        return _lane_semantic_split(text, profile)
    return _schema_only_split(text, profile)


def split_blocks(
    blocks: list[tuple[str, str, str]],
    profile: ChunkingProfile,
) -> list[TextSlice]:
    out: list[TextSlice] = []
    for hp, title, body in blocks:
        for piece in split_by_strategy(body, profile):
            out.append(TextSlice(text=piece, section_title=title, hierarchy_path=hp))
    return out


def split_and_cap_blocks(
    blocks: list[tuple[str, str, str]],
    profile: ChunkingProfile,
) -> list[TextSlice]:
    return cap_slices(split_blocks(blocks, profile), profile)

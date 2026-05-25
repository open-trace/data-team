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
from ml.rag.text_processors.preprocess.structure_blocks import StructureBlock
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


_COLUMN_DOC_MARKER_RE = re.compile(
    r"Column Name\s*\||^Column Name:|^Column\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def _split_bq_schema_block(text: str, profile: ChunkingProfile) -> list[str]:
    """Split column/schema docs on row boundaries, never mid-row."""
    text = text.strip()
    if not text:
        return []
    model_id = profile.embedding_model
    target = profile.target_tokens
    if count_tokens(text, model_id=model_id) <= target:
        return [text]

    lines = text.splitlines()
    header_lines: list[str] = []
    body_lines: list[str] = []
    for line in lines:
        if line.strip().startswith("|") or re.match(r"^Column Name", line.strip(), re.I):
            body_lines.append(line)
        elif not body_lines:
            header_lines.append(line)
        else:
            body_lines.append(line)

    if not body_lines:
        return _enforce_max_tokens(_split_bq_prose_block(text, profile), profile)

    chunks: list[str] = []
    buf: list[str] = list(header_lines)
    if header_lines and body_lines:
        buf.append("")

    def flush_buf() -> None:
        nonlocal buf
        body = "\n".join(buf).strip()
        buf = list(header_lines)
        if header_lines and body_lines:
            buf.append("")
        if body:
            chunks.append(body)

    for line in body_lines:
        candidate = "\n".join([*buf, line]).strip()
        if buf and count_tokens(candidate, model_id=model_id) > target:
            flush_buf()
        buf.append(line)
    flush_buf()
    return chunks or [text]


def _split_bq_prose_block(text: str, profile: ChunkingProfile) -> list[str]:
    """Overview/insights: paragraph boundaries first, then token cap."""
    pieces: list[str] = []
    for para in _paragraphs(text):
        pt = count_tokens(para, model_id=profile.embedding_model)
        if pt <= profile.target_tokens:
            pieces.append(para)
        else:
            pieces.extend(_split_sentences_token_aware(para, profile))
    return _enforce_max_tokens(pieces, profile)


def _bq_structured_split(text: str, profile: ChunkingProfile) -> list[str]:
    if _COLUMN_DOC_MARKER_RE.search(text) or sum(
        1 for ln in text.splitlines() if ln.strip().startswith("|") and ln.count("|") >= 2
    ) >= 2:
        return _split_bq_schema_block(text, profile)
    return _split_bq_prose_block(text, profile)


def split_by_strategy(text: str, profile: ChunkingProfile) -> list[str]:
    strategy: ChunkingStrategy = profile.chunking_strategy
    if strategy == "recursive_semantic":
        return _recursive_semantic_split(text, profile)
    if strategy == "hierarchical_semantic":
        return _hierarchical_semantic_split(text, profile)
    if strategy == "lane_semantic":
        return _lane_semantic_split(text, profile)
    if strategy == "bq_structured":
        return _bq_structured_split(text, profile)
    if strategy == "schema_only":
        return _schema_only_split(text, profile)
    return _schema_only_split(text, profile)


def _schema_only_split(text: str, profile: ChunkingProfile) -> list[str]:
    """Legacy BQ: sentence + token cap only."""
    return _enforce_max_tokens(_split_sentences_token_aware(text, profile), profile)


def _table_aware_split(text: str, profile: ChunkingProfile) -> list[str]:
    """Keep tables intact when possible; token-split only when oversized."""
    text = text.strip()
    if not text:
        return []
    model_id = profile.embedding_model
    max_table_tokens = profile.target_tokens * 2
    if count_tokens(text, model_id=model_id) <= max_table_tokens:
        return [text]
    return _enforce_max_tokens(_split_sentences_token_aware(text, profile), profile)


def split_structure_block(block: StructureBlock, profile: ChunkingProfile) -> list[TextSlice]:
    if block.content_type == "table":
        if profile.chunking_strategy == "bq_structured":
            pieces = _split_bq_schema_block(block.text, profile)
        else:
            pieces = _table_aware_split(block.text, profile)
    else:
        pieces = split_by_strategy(block.text, profile)
    return [
        TextSlice(
            text=piece,
            section_title=block.section_title,
            hierarchy_path=block.hierarchy_path,
            content_type=block.content_type,
        )
        for piece in pieces
    ]


def split_structure_blocks(
    blocks: list[StructureBlock],
    profile: ChunkingProfile,
) -> list[TextSlice]:
    out: list[TextSlice] = []
    for block in blocks:
        out.extend(split_structure_block(block, profile))
    return out


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

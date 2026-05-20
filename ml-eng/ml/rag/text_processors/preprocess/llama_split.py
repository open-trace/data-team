from __future__ import annotations

import re
from dataclasses import dataclass

from ml.rag.text_processors.chunking_config import ChunkingProfile
from ml.rag.text_processors.preprocess.tokens import count_tokens


@dataclass(frozen=True)
class TextSlice:
    text: str
    section_title: str = ""
    hierarchy_path: str = ""


def _split_sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [s.strip() for s in parts if s.strip()]


def _overlap_tail_words(words: list[str], overlap_tokens: int, model_id: str) -> list[str]:
    if overlap_tokens <= 0 or not words:
        return []
    tail: list[str] = []
    tail_tok = 0
    for w in reversed(words):
        wt = count_tokens((w + " "), model_id=model_id)
        if tail_tok + wt > overlap_tokens:
            break
        tail.insert(0, w)
        tail_tok += wt
    return tail


def _split_by_words(text: str, profile: ChunkingProfile) -> list[str]:
    """Hard split by word windows when sentences are too long."""
    target = profile.target_tokens
    overlap = profile.overlap_tokens
    model_id = profile.embedding_model
    words = text.split()
    if not words:
        return []

    chunks: list[str] = []
    buf: list[str] = []
    buf_tok = 0

    for w in words:
        wt = count_tokens(w + " ", model_id=model_id)
        if buf_tok + wt > target and buf:
            chunks.append(" ".join(buf))
            buf = _overlap_tail_words(buf, overlap, model_id)
            buf_tok = count_tokens(" ".join(buf) + (" " if buf else ""), model_id=model_id)
        buf.append(w)
        buf_tok += wt

    if buf:
        chunks.append(" ".join(buf))
    return chunks


def _split_sentences_token_aware(text: str, profile: ChunkingProfile) -> list[str]:
    target = profile.target_tokens
    overlap = profile.overlap_tokens
    model_id = profile.embedding_model

    sents = _split_sentences(text)
    if not sents:
        return _split_by_words(text, profile)

    chunks: list[str] = []
    buf: list[str] = []
    buf_tok = 0

    def flush() -> None:
        nonlocal buf, buf_tok
        if buf:
            chunks.append(" ".join(buf).strip())
        buf = []
        buf_tok = 0

    for sent in sents:
        st = count_tokens(sent, model_id=model_id)
        if st > target:
            flush()
            chunks.extend(_split_by_words(sent, profile))
            continue
        if buf_tok + st > target and buf:
            flush()
            if overlap > 0 and chunks:
                prev_words = chunks[-1].split()
                tail = _overlap_tail_words(prev_words, overlap, model_id)
                if tail:
                    sent = f"{' '.join(tail)} {sent}".strip()
                    st = count_tokens(sent, model_id=model_id)
        buf.append(sent)
        buf_tok += st

    flush()
    return chunks


def _enforce_max_tokens(pieces: list[str], profile: ChunkingProfile) -> list[str]:
    """Guarantee every piece is <= target_tokens (split further if needed)."""
    target = profile.target_tokens
    model_id = profile.embedding_model
    out: list[str] = []
    for piece in pieces:
        piece = piece.strip()
        if not piece:
            continue
        if count_tokens(piece, model_id=model_id) <= target:
            out.append(piece)
            continue
        for sub in _split_sentences_token_aware(piece, profile):
            if count_tokens(sub, model_id=model_id) <= target:
                out.append(sub)
            else:
                out.extend(_split_by_words(sub, profile))
    return out


def split_text_to_slices(text: str, profile: ChunkingProfile) -> list[str]:
    """
    Corpus-specific chunking (recursive/semantic, hierarchical, lane, schema-only)
    then hard token cap. See chunking_config.ChunkingStrategy.
    """
    from ml.rag.text_processors.preprocess.split_strategy import split_by_strategy

    return split_by_strategy(text, profile)


def split_blocks(
    blocks: list[tuple[str, str, str]],
    profile: ChunkingProfile,
) -> list[TextSlice]:
    from ml.rag.text_processors.preprocess.split_strategy import split_blocks as _strategy_split_blocks

    return _strategy_split_blocks(blocks, profile)


def cap_slices(slices: list[TextSlice], profile: ChunkingProfile) -> list[TextSlice]:
    """
    Limit chunks per document without creating one oversized tail blob.
    Extra slices beyond cap are dropped (prefer quality-sized chunks).
    """
    cap = profile.max_chunks_per_doc
    if cap is None or len(slices) <= cap:
        return slices
    return slices[:cap]

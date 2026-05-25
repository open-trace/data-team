"""
Chat completion helpers. Prefer ``ml.rag.llm_chat.llm_chat_complete`` for all RAG nodes.
"""
from __future__ import annotations

from typing import Any

from ml.rag.llm_chat import llm_chat_complete, llm_model_id


def hf_chat_sync(
    messages: list[dict[str, Any]],
    *,
    model: str,
    max_tokens: int = 512,
    temperature: float = 0.0,
) -> str:
    """Non-streaming chat completion; returns assistant text or empty string on failure."""
    return llm_chat_complete(
        messages,
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
    )


def hf_chat_stream_aggregate(
    messages: list[dict[str, Any]],
    *,
    model: str,
    max_tokens: int = 512,
    temperature: float = 0.3,
) -> str:
    """Streaming not used for local/OpenAI-compatible backends; falls back to sync."""
    return llm_chat_complete(
        messages,
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
    )

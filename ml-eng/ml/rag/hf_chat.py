"""
Hugging Face chat completions via ``huggingface_hub.InferenceClient`` (not raw HTTP).

- Non-streaming: NL-to-SQL, decomposer, reranker, chat memory, ``check_hf`` smoke test.
- Streaming: generator only — chunks are aggregated into a single string before return.
"""
from __future__ import annotations

from typing import Any

from huggingface_hub import InferenceClient

from ml.rag.hf_token import get_hf_api_token


def _inference_client() -> InferenceClient | None:
    token = get_hf_api_token()
    if not token:
        return None
    return InferenceClient(api_key=token)


def hf_chat_sync(
    messages: list[dict[str, Any]],
    *,
    model: str,
    max_tokens: int = 512,
    temperature: float = 0.0,
) -> str:
    """
    Single non-streaming chat completion; returns assistant text or empty string on failure.
    """
    client = _inference_client()
    if not client:
        return ""
    try:
        out = client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=False,
        )
        choices = getattr(out, "choices", None)
        if choices is None and isinstance(out, dict):
            choices = out.get("choices")
        if not choices:
            return ""
        choice0 = choices[0]
        msg = getattr(choice0, "message", None)
        if msg is None and isinstance(choice0, dict):
            msg = choice0.get("message")
        if isinstance(msg, dict):
            content = msg.get("content")
        else:
            content = getattr(msg, "content", None) if msg is not None else None
        return str(content or "").strip()
    except Exception:
        return ""


def hf_chat_stream_aggregate(
    messages: list[dict[str, Any]],
    *,
    model: str,
    max_tokens: int = 512,
    temperature: float = 0.3,
) -> str:
    """
    Streaming completion; concatenates text deltas (used by generator only).
    """
    client = _inference_client()
    if not client:
        return ""
    try:
        stream = client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=True,
        )
        parts: list[str] = []
        for chunk in stream:
            choices = getattr(chunk, "choices", None)
            if choices is None and isinstance(chunk, dict):
                choices = chunk.get("choices")
            if not choices:
                continue
            ch0 = choices[0]
            delta = getattr(ch0, "delta", None)
            if delta is None and isinstance(ch0, dict):
                delta = ch0.get("delta")
            if isinstance(delta, dict):
                piece = delta.get("content")
            else:
                piece = getattr(delta, "content", None) if delta is not None else None
            if piece:
                parts.append(piece)
        return "".join(parts).strip()
    except Exception:
        return ""

from __future__ import annotations

try:
    import tiktoken
except ImportError:  # pragma: no cover
    tiktoken = None  # type: ignore[assignment,misc]

_CHARS_PER_TOKEN = 4


def count_tokens(text: str, *, model_id: str = "") -> int:
    if tiktoken is not None:
        try:
            enc = tiktoken.get_encoding("cl100k_base")
            return len(enc.encode(text or ""))
        except Exception:
            pass
    return max(1, len(text or "") // _CHARS_PER_TOKEN)

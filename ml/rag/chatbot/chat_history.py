"""
Truncate prior chat messages for the generator prompt (bounded turn + char caps).
Retrieval still uses only the latest user message; this is for generation memory only.
"""
from __future__ import annotations

import os
from typing import Any


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def default_max_turn_pairs() -> int:
    """Max user+assistant pairs to keep verbatim (default 5 full back-and-forth turns)."""
    return _env_int("RAG_CHAT_HISTORY_MAX_TURNS", 5)


def default_max_chars() -> int:
    """Max total characters for the formatted prior-conversation block."""
    return _env_int("RAG_CHAT_HISTORY_MAX_CHARS", 4000)


def normalize_messages(messages: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    if not messages:
        return []
    out: list[dict[str, str]] = []
    for m in messages:
        if not isinstance(m, dict):
            continue
        role = str(m.get("role", "")).strip().lower()
        if role not in ("user", "assistant"):
            continue
        content = str(m.get("content", "")).strip()
        if not content:
            continue
        out.append({"role": role, "content": content})
    return out


def truncate_chat_history(
    messages: list[dict[str, Any]] | None,
    max_turn_pairs: int | None = None,
    max_chars: int | None = None,
) -> list[dict[str, str]]:
    """
    Keep the most recent conversation, capped by pair count then by total character length.
    One "turn pair" is one user message plus one assistant message (two list entries).
    """
    norm = normalize_messages(messages)
    max_pairs = default_max_turn_pairs() if max_turn_pairs is None else max(0, max_turn_pairs)
    cap_chars = default_max_chars() if max_chars is None else max(0, max_chars)
    if max_pairs == 0 or not norm:
        return []

    max_msgs = max_pairs * 2
    tail = norm[-max_msgs:]

    def block_chars(msgs: list[dict[str, str]]) -> int:
        return sum(len(m["role"]) + len(m["content"]) + 4 for m in msgs)

    while len(tail) > 1 and block_chars(tail) > cap_chars:
        tail = tail[2:]

    while block_chars(tail) > cap_chars and tail:
        tail = tail[1:]

    return tail


def format_chat_history_block(messages: list[dict[str, str]]) -> str:
    if not messages:
        return ""
    lines: list[str] = ["Prior conversation (most recent last):"]
    for m in messages:
        label = "User" if m["role"] == "user" else "Assistant"
        lines.append(f"{label}: {m['content']}")
    return "\n".join(lines)

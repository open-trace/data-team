"""
Rolling chat memory: keep the last N user+assistant pairs verbatim; fold older pairs
into one running summary via LLM (or a text stub if HF is unavailable).
"""
from __future__ import annotations

import os
from typing import Any

import requests

from ml.rag.chat_history import normalize_messages

_SUMMARY_SYSTEM = (
    "You compress chat history into a single factual running summary. "
    "Preserve entities, regions, countries, crops, years, metrics, and the user's goals. "
    "No bullet fluff; tight prose. Output only the summary text, no preamble."
)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def default_verbatim_pairs() -> int:
    """Max user+assistant pairs kept verbatim (default 5)."""
    v = _env_int("RAG_CHAT_VERBATIM_TURNS", 0)
    if v > 0:
        return v
    return _env_int("RAG_CHAT_HISTORY_MAX_TURNS", 5)


def default_summary_max_chars() -> int:
    return _env_int("RAG_SUMMARY_MAX_CHARS", 2000)


def default_verbatim_max_chars() -> int:
    return _env_int("RAG_CHAT_HISTORY_MAX_CHARS", 4000)


def count_complete_pairs(messages: list[dict[str, str]]) -> int:
    i = 0
    n = 0
    while i + 1 < len(messages):
        if messages[i]["role"] == "user" and messages[i + 1]["role"] == "assistant":
            n += 1
            i += 2
        else:
            i += 1
    return n


def pop_oldest_pair(messages: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Remove and return the first user+assistant pair; drop leading orphans."""
    rest = list(messages)
    while rest and rest[0]["role"] != "user":
        rest = rest[1:]
    if len(rest) < 2 or rest[1]["role"] != "assistant":
        return [], messages
    pair = rest[:2]
    return pair, rest[2:]


def _pair_to_text(pair: list[dict[str, str]]) -> str:
    lines = []
    for m in pair:
        label = "User" if m["role"] == "user" else "Assistant"
        lines.append(f"{label}: {m['content']}")
    return "\n".join(lines)


def merge_summary_llm(existing_summary: str, evicted_pair: list[dict[str, str]]) -> str:
    """Return merged summary; empty string if caller should use stub."""
    api_token = os.environ.get("HF_API_TOKEN")
    model_id = os.environ.get("RAG_SUMMARY_MODEL_ID") or os.environ.get(
        "RAG_LLM_MODEL_ID", "meta-llama/Llama-3.1-8B-Instruct"
    )
    if not api_token:
        return ""

    new_text = _pair_to_text(evicted_pair)
    user_block = (
        f"Current running summary (may be empty):\n{existing_summary.strip()}\n\n"
        f"Oldest turn to fold in:\n{new_text}\n\n"
        "Write the updated running summary only."
    )
    url = "https://router.huggingface.co/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model_id,
        "messages": [
            {"role": "system", "content": _SUMMARY_SYSTEM},
            {"role": "user", "content": user_block},
        ],
        "max_tokens": 512,
        "temperature": 0.2,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        if resp.status_code in (410, 503, 502, 429):
            return ""
        resp.raise_for_status()
        data = resp.json()
        out = str(data["choices"][0]["message"]["content"]).strip()
        cap = default_summary_max_chars()
        return out[:cap] if out else ""
    except Exception:
        return ""


def fold_pair_into_summary(existing_summary: str, evicted_pair: list[dict[str, str]]) -> str:
    merged = merge_summary_llm(existing_summary, evicted_pair)
    if merged:
        return merged[: default_summary_max_chars()]
    fold = _pair_to_text(evicted_pair)
    base = (existing_summary.strip() + "\n\n") if existing_summary.strip() else ""
    stub = base + "[Earlier conversation (no summarization LLM):] " + fold[:800]
    return stub[: default_summary_max_chars()]


def append_turn_and_compact(
    conversation_summary: str,
    recent_turns: list[dict[str, Any]] | None,
    user_content: str,
    assistant_content: str,
) -> tuple[str, list[dict[str, str]]]:
    """
    Append the new user+assistant pair, then fold oldest pairs into summary until
    at most default_verbatim_pairs() complete pairs remain.
    """
    recent = normalize_messages(recent_turns)
    recent.append({"role": "user", "content": user_content.strip()})
    recent.append({"role": "assistant", "content": assistant_content.strip()})
    summary = (conversation_summary or "").strip()
    max_pairs = default_verbatim_pairs()
    if max_pairs == 0:
        while recent:
            pair, recent = pop_oldest_pair(recent)
            if not pair:
                break
            summary = fold_pair_into_summary(summary, pair)
        return summary, recent

    while count_complete_pairs(recent) > max_pairs:
        pair, rest = pop_oldest_pair(recent)
        if not pair:
            recent = rest
            if not recent:
                break
            continue
        summary = fold_pair_into_summary(summary, pair)
        recent = rest

    while True:
        block = build_memory_prompt_block(summary, recent)
        if len(block) <= default_verbatim_max_chars() or len(recent) < 3:
            break
        pair, rest = pop_oldest_pair(recent)
        if not pair:
            break
        summary = fold_pair_into_summary(summary, pair)
        recent = rest

    return summary, recent


def build_memory_prompt_block(
    conversation_summary: str,
    recent_turns: list[dict[str, str]] | None,
) -> str:
    parts: list[str] = []
    s = (conversation_summary or "").strip()
    if s:
        parts.append(f"Conversation summary (compressed earlier chat):\n{s}")
    recent = normalize_messages(recent_turns)
    if recent:
        lines = ["Recent conversation (verbatim, most recent last):"]
        for m in recent:
            label = "User" if m["role"] == "user" else "Assistant"
            lines.append(f"{label}: {m['content']}")
        parts.append("\n".join(lines))
    if not parts:
        return ""
    return "\n\n".join(parts)


def flat_messages_to_memory(messages: list[dict[str, Any]] | None) -> tuple[str, list[dict[str, str]]]:
    """Fold a flat transcript into (summary, recent_turns) with at most verbatim pairs."""
    norm = normalize_messages(messages)
    if not norm:
        return "", []
    summary = ""
    recent: list[dict[str, str]] = []
    i = 0
    while i < len(norm):
        if norm[i]["role"] != "user":
            recent.append(norm[i])
            i += 1
            continue
        if i + 1 >= len(norm) or norm[i + 1]["role"] != "assistant":
            recent.extend(norm[i:])
            break
        summary, recent = append_turn_and_compact(
            summary,
            recent,
            norm[i]["content"],
            norm[i + 1]["content"],
        )
        i += 2
    return summary, recent

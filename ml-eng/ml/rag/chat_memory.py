"""
Stable import path ``ml.rag.chat_memory``; implementation lives in ``ml.rag.chatbot.chat_memory``.
"""

from __future__ import annotations

from ml.rag.chatbot.chat_memory import (
    append_turn_and_compact,
    build_memory_prompt_block,
    count_complete_pairs,
    default_summary_max_chars,
    default_verbatim_max_chars,
    default_verbatim_pairs,
    flat_messages_to_memory,
    fold_pair_into_summary,
    merge_summary_llm,
    pop_oldest_pair,
)

__all__ = [
    "append_turn_and_compact",
    "build_memory_prompt_block",
    "count_complete_pairs",
    "default_summary_max_chars",
    "default_verbatim_max_chars",
    "default_verbatim_pairs",
    "flat_messages_to_memory",
    "fold_pair_into_summary",
    "merge_summary_llm",
    "pop_oldest_pair",
]

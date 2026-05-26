"""
Generator node: takes query + reranked context and produces the final answer (LLM).
Uses Hugging Face router API (chat completions) with
`meta-llama/Llama-3.1-8B-Instruct` by default.
"""
from __future__ import annotations

import json
import os
from typing import Any

from ml.rag.llm_chat import llm_chat_complete, llm_configured, llm_default_timeout_s, llm_model_id

from ml.rag.chat_history import normalize_messages, truncate_chat_history
from ml.rag.chat_memory import (
    build_memory_prompt_block,
    default_summary_max_chars,
    default_verbatim_max_chars,
)


def _build_prompt(
    query: str,
    context_block: str,
    decomposition: dict[str, Any] | None = None,
    memory_block: str = "",
) -> list[dict[str, str]]:
    system = (
        "You are a helpful data assistant for the OpenTrace team. Questions are often about "
        "agricultural production, crop productivity, regions, districts, agroecological zones, "
        "yield gaps, rainfall, irrigation, drought, food supply stability, and trends over time. "
        "Use the provided context (BigQuery results and/or document snippets) to answer. "
        "Ground your answer in the context: prefer facts from [News], [Academic], [Policy], "
        "[Public report], and BigQuery row text. "
        "For [Academic] snippets, cite the source when metadata is present (authors, year, article title, journal, DOI). "
        "Cite specific numbers or regions when the context supports it. "
        "If the context does not fully answer the question, say so and summarize what the data does show. "
        "Do not invent citations or statistics that are not supported by the context."
    )
    facet_block = ""
    intent_tone = ""
    if decomposition:
        intent = str(decomposition.get("intent") or "").strip().lower()
        if intent == "predictive":
            intent_tone = (
                " The user's primary intent is forward-looking: clearly separate what the context shows "
                "from speculation; state uncertainty and limits of the data; avoid presenting guesses as facts."
            )
        elif intent == "diagnostic":
            intent_tone = (
                " The user's primary intent asks why or what drives outcomes: do not claim causation "
                "unless the context explicitly supports it; distinguish correlation from causation."
            )
        try:
            facet_block = (
                "\nExtracted query facets (for alignment; may be incomplete):\n"
                + json.dumps(decomposition, ensure_ascii=False)[:2000]
                + "\n"
            )
        except Exception:
            facet_block = ""
    if intent_tone:
        system = system + intent_tone
    mb = (memory_block.strip() + "\n\n") if memory_block.strip() else ""
    user = f"{facet_block}{mb}Context:\n{context_block}\n\nQuestion: {query}"
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def _resolve_memory_block(**kwargs: Any) -> str:
    conv_summary = kwargs.get("conversation_summary")
    recent_turns = kwargs.get("recent_turns")
    raw_history = kwargs.get("chat_history")
    block = ""
    if isinstance(recent_turns, list) or isinstance(conv_summary, str):
        s = (conv_summary if isinstance(conv_summary, str) else "").strip()
        rt = normalize_messages(recent_turns if isinstance(recent_turns, list) else None)
        block = build_memory_prompt_block(s, rt)
    elif isinstance(raw_history, list) and raw_history:
        rt = truncate_chat_history(raw_history)
        block = build_memory_prompt_block("", rt)
    cap = default_verbatim_max_chars() + default_summary_max_chars()
    if len(block) > cap:
        block = block[-cap:]
    return block


def _call_llama(messages: list[dict[str, str]]) -> str:
    """Call configured LLM backend; never raises on HTTP errors."""
    gen_timeout = float(os.environ.get("RAG_GENERATE_TIMEOUT_S", "0") or 0) or llm_default_timeout_s()
    max_toks = int(os.environ.get("RAG_GENERATE_MAX_TOKENS", "1024") or 1024)
    return llm_chat_complete(
        messages,
        model=llm_model_id(),
        max_tokens=max_toks,
        temperature=0.3,
        timeout_s=gen_timeout,
    )


def generate(
    query: str,
    context_items: list[dict[str, Any]],
    **kwargs: Any,
) -> str:
    """
    Produce an answer from query and context.

    - If HF_API_TOKEN is set, calls LLM via Hugging Face router (chat completions).
    - Otherwise, falls back to a simple debug-style answer that echoes the context.
    """
    decomposition = kwargs.get("decomposition")
    if not isinstance(decomposition, dict):
        decomposition = None

    memory_block = _resolve_memory_block(**kwargs)

    if not context_items:
        allow_ungrounded = os.environ.get("RAG_ALLOW_UNGROUNDED", "").strip().lower() in (
            "1",
            "true",
            "on",
            "yes",
        )
        if allow_ungrounded:
            messages = _build_prompt(
                query,
                context_block="[No external context]",
                decomposition=decomposition,
                memory_block=memory_block,
            )
            llama_answer = _call_llama(messages)
            if llama_answer:
                return llama_answer
        return (
            "I couldn't find relevant OpenTrace sources (news, research, policy, public reports, "
            "or BigQuery data) for this question. Try naming a specific country, crop, or dataset, "
            "or confirm the Qdrant collections are loaded."
        )

    content_key = "content" if any("content" in c for c in context_items) else "text"
    ctx_budget = 6000
    if memory_block:
        ctx_budget = max(2000, 6000 - len(memory_block))
    context_block = "\n\n".join(
        (c.get(content_key) or c.get("text", str(c)))[:2000] for c in context_items
    )[:ctx_budget]

    messages = _build_prompt(query, context_block, decomposition=decomposition, memory_block=memory_block)
    llama_answer = _call_llama(messages)
    if llama_answer:
        return llama_answer

    if llm_configured():
        hint = (
            "[LLM generation failed — local server may have timed out or the request was cancelled. "
            f"Try RAG_LLM_TIMEOUT_S=300, RAG_GENERATE_MAX_TOKENS=1024, and RAG_LLM_RERANK=off. "
            f"Model id must match LM Studio: {llm_model_id()!r}. Showing retrieved context only.]\n\n"
        )
    else:
        hint = (
            "[LLM unavailable — set RAG_LLM_BASE_URL (e.g. http://127.0.0.1:1234/v1) for LM Studio, "
            "or HF_API_TOKEN for the Hugging Face router. Showing retrieved context only.]\n\n"
        )
    return hint + f"Context:\n{context_block[:3000]}\n\nQuery: {query}"

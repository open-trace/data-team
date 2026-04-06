"""
Generator node: takes query + reranked context and produces the final answer (LLM).
Uses Hugging Face router API (chat completions) with
`meta-llama/Llama-3.1-8B-Instruct` by default.
"""
from __future__ import annotations

import json
import os
from typing import Any

import requests

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
) -> str:
    system = (
        "You are a helpful data assistant for the OpenTrace team. Questions are often about "
        "agricultural production, crop productivity, regions, districts, agroecological zones, "
        "yield gaps, rainfall, irrigation, drought, food supply stability, and trends over time. "
        "Use the provided context (BigQuery results and/or document snippets) to answer. "
        "Ground your answer in the context: prefer facts from [News], [Academic], and BigQuery row text. "
        "Cite specific numbers or regions when the context supports it. "
        "If the context does not fully answer the question, say so and summarize what the data does show."
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
    return (
        f"<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n"
        f"{system}\n"
        f"<|eot_id|><|start_header_id|>user<|end_header_id|>\n"
        f"{facet_block}"
        f"{mb}"
        f"Context:\n{context_block}\n\nQuestion: {query}\n"
        f"<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n"
    )


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


def _call_llama(prompt: str) -> str:
    """Call LLM via Hugging Face router (chat completions). Falls back if not configured."""
    api_token = os.environ.get("HF_API_TOKEN")
    model_id = os.environ.get(
        "RAG_LLM_MODEL_ID", "meta-llama/Llama-3.1-8B-Instruct"
    )
    if not api_token:
        return ""

    url = "https://router.huggingface.co/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 512,
        "temperature": 0.3,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    if resp.status_code in (410, 503, 502, 429):
        return ""
    resp.raise_for_status()
    data = resp.json()
    try:
        return str(data["choices"][0]["message"]["content"]).strip()
    except (KeyError, IndexError, TypeError):
        return ""


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
        # Still try to call the LLM so it can answer from its own knowledge if allowed.
        prompt = _build_prompt(
            query,
            context_block="[No external context]",
            decomposition=decomposition,
            memory_block=memory_block,
        )
        llama_answer = _call_llama(prompt)
        if llama_answer:
            return llama_answer
        return f"[No context] Query: {query}"

    content_key = "content" if any("content" in c for c in context_items) else "text"
    ctx_budget = 6000
    if memory_block:
        ctx_budget = max(2000, 6000 - len(memory_block))
    context_block = "\n\n".join(
        (c.get(content_key) or c.get("text", str(c)))[:2000] for c in context_items
    )[:ctx_budget]

    prompt = _build_prompt(query, context_block, decomposition=decomposition, memory_block=memory_block)
    llama_answer = _call_llama(prompt)
    if llama_answer:
        return llama_answer

    # Fallback when HF_API_TOKEN missing or API returned 410/503/etc.
    return (
        f"[LLM unavailable—showing retrieved context only.]\n\n"
        f"Context:\n{context_block[:3000]}\n\nQuery: {query}"
    )

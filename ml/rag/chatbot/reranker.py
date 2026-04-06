"""
Reranker node: takes merged retrieval results and reranks them (e.g. cross-encoder or LLM)
before passing to the generator.

Uses Hugging Face router (chat completions) with Llama 3.1 8B by default.
If not configured, falls back to a simple pass-through (first top_k items).
"""
from __future__ import annotations

import os
from typing import Any

import requests


def _score_with_llama(query: str, text: str) -> float:
    """
    Ask Llama to score how relevant `text` is to `query` on [0, 1].
    This is a simple, low-throughput reranker: one call per chunk.
    """
    api_token = os.environ.get("HF_API_TOKEN")
    model_id = os.environ.get(
        "RAG_LLM_MODEL_ID", "meta-llama/Llama-3.1-8B-Instruct"
    )
    if not api_token:
        return -1.0

    url = "https://router.huggingface.co/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    prompt = (
        "You are a ranking model. Given a user question and a context chunk, "
        "return a single floating point number between 0 and 1 indicating how relevant "
        "the context is to answering the question. Respond with ONLY the number.\n\n"
        f"Question: {query}\n\n"
        f"Context:\n{text}\n\n"
        "Relevance score (0-1):"
    )
    payload = {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 8,
        "temperature": 0.0,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        raw = str(data["choices"][0]["message"]["content"]).strip()
        token = raw.split()[0]
        return float(token)
    except Exception:
        return -1.0


def rerank(
    query: str,
    context_items: list[dict[str, Any]],
    top_k: int = 5,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """
    Rerank context_items by relevance to query. Each item should have "content" (or "text").

    - If HF_API_TOKEN is set and RAG_LLM_RERANK != \"off\", uses LLM via HF router to score each chunk.
    - Otherwise, returns first top_k items (original order).
    """
    if not context_items:
        return []

    use_llm = os.environ.get("RAG_LLM_RERANK", "on").lower() not in {"off", "0", "false"}
    content_key = "content" if any("content" in c for c in context_items) else "text"

    _SOURCE_BOOST = {"bigquery": 0.12, "news": 0.04, "academic": 0.06}

    if not use_llm or not os.environ.get("HF_API_TOKEN"):
        # Simple baseline: preserve ordering, just trim to top_k (with source-aware score for display)
        scored = []
        for i, item in enumerate(context_items):
            text = item.get(content_key) or item.get("text", str(item))
            kind = item.get("_context_kind") or item.get("source") or ""
            boost = _SOURCE_BOOST.get(str(kind).lower(), 0.0)
            scored.append(
                {
                    **item,
                    "content": text,
                    "_order": i,
                    "_llm_score": -1.0,
                    "_source_boost": boost,
                    "_rerank_score": boost,
                }
            )
        scored.sort(key=lambda x: x.get("_rerank_score", 0.0), reverse=True)
        return scored[:top_k]

    # Light source-aware boost so structured BQ rows are not drowned by long text chunks

    scored = []
    for i, item in enumerate(context_items):
        text = (item.get(content_key) or item.get("text", str(item)))[:2000]
        score = _score_with_llama(query, text)
        kind = item.get("_context_kind") or item.get("source") or ""
        boost = _SOURCE_BOOST.get(str(kind).lower(), 0.0)
        adjusted = score + boost if score >= 0 else score
        scored.append(
            {
                **item,
                "content": text,
                "_order": i,
                "_llm_score": score,
                "_source_boost": boost,
                "_rerank_score": adjusted,
            }
        )

    scored.sort(key=lambda x: x.get("_rerank_score", x.get("_llm_score", -1.0)), reverse=True)
    return scored[:top_k]

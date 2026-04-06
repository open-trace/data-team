"""
Shared state for the RAG graph. All nodes read from and optionally update this state.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RAGState:
    """State passed through the RAG graph."""

    query: str
    """User question or query."""

    bq_results: list[dict[str, Any]] = field(default_factory=list)
    """Results from BigQuery retrieval (e.g. rows or text snippets)."""

    vector_results: list[dict[str, Any]] = field(default_factory=list)
    """Results from vector DB retrieval (e.g. chunks with score, metadata)."""

    merged_context: list[dict[str, Any]] = field(default_factory=list)
    """Combined retrieval results before reranking."""

    reranked_context: list[dict[str, Any]] = field(default_factory=list)
    """Context after reranker; fed to generator."""

    answer: str = ""
    """Final generated answer."""

    error: str | None = None
    """If set, a node encountered an error."""

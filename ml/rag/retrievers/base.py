"""
Base retriever interface. All retrievers take a query and return a list of context items.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseRetriever(ABC):
    """Interface for retrieval nodes in the RAG graph."""

    @abstractmethod
    def retrieve(self, query: str, top_k: int = 10, **kwargs: Any) -> list[dict[str, Any]]:
        """
        Return a list of context items. Each item is a dict with at least:
        - "content" or "text": string used as context
        - optional "score", "metadata", "source", etc.
        """
        ...

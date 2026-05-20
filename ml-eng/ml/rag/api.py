"""Uvicorn entrypoint: ``uvicorn ml.rag.api:app`` (implementation in ``ml.rag.app.api``)."""

from __future__ import annotations

from ml.rag.app.api import app

__all__ = ["app"]

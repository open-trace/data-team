"""
Stable chunk / document identifiers and content hashing for idempotent Qdrant upserts.
"""
from __future__ import annotations

import hashlib
import os
import uuid
from typing import Any

from ml.rag.text_processors.chunking_config import CHUNK_ID_NAMESPACE, INGEST_VERSION, CorpusKey

_NS = uuid.UUID(CHUNK_ID_NAMESPACE)


def normalize_chunk_text(text: str) -> str:
    return " ".join((text or "").split())


def content_hash(text: str) -> str:
    return hashlib.sha256(normalize_chunk_text(text).encode("utf-8")).hexdigest()


def document_id_from_path(path: str, *, dedupe_id: str | None = None) -> str:
    if dedupe_id and str(dedupe_id).strip():
        return str(dedupe_id).strip()
    return hashlib.sha256(str(path).encode("utf-8")).hexdigest()[:32]


def resolve_namespace(*, namespace: str | None = None, corpus: CorpusKey | str | None = None) -> str:
    if namespace and str(namespace).strip():
        return str(namespace).strip()
    if corpus:
        env_key = f"RAG_NAMESPACE_{str(corpus).upper()}"
        env_value = os.environ.get(env_key, "").strip()
        if env_value:
            return env_value
    env_value = os.environ.get("RAG_NAMESPACE", "").strip()
    if env_value:
        return env_value
    return str(corpus or "default").strip() or "default"


def make_chunk_id(
    *,
    corpus: CorpusKey,
    document_id: str,
    chunk_index: int,
    text: str,
    namespace: str | None = None,
) -> str:
    ch = content_hash(text)
    namespace_value = resolve_namespace(namespace=namespace, corpus=corpus)
    return str(
        uuid.uuid5(
            _NS,
            f"{corpus}|{namespace_value}|{document_id}|{chunk_index}|{ch[:16]}",
        )
    )


def enrich_metadata(
    meta: dict[str, Any],
    *,
    corpus: CorpusKey,
    document_id: str,
    chunk_index: int,
    total_chunks: int,
    text: str,
    section_path: str = "",
    section_title: str = "",
    hierarchy_path: str = "",
    parent_chunk_id: str | None = None,
    semantic_lane: str = "",
    section_role: str = "",
    content_type: str = "",
    namespace: str | None = None,
) -> dict[str, Any]:
    out = dict(meta)
    out["document_id"] = document_id
    out["chunk_index"] = chunk_index
    out["total_chunks"] = total_chunks
    out["content_hash"] = content_hash(text)
    out["ingest_version"] = INGEST_VERSION
    namespace_value = resolve_namespace(namespace=namespace, corpus=corpus)
    out["namespace"] = namespace_value
    if section_path:
        out["section_path"] = section_path
    if section_title:
        out["section_title"] = section_title
    if hierarchy_path:
        out["hierarchy_path"] = hierarchy_path
    if parent_chunk_id:
        out["parent_chunk_id"] = parent_chunk_id
    if semantic_lane:
        out["semantic_lane"] = semantic_lane
    if section_role:
        out["section_role"] = section_role
    if content_type:
        out["content_type"] = content_type
    out["id"] = make_chunk_id(
        corpus=corpus,
        document_id=document_id,
        chunk_index=chunk_index,
        text=text,
        namespace=namespace_value,
    )
    return out

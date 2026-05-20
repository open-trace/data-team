"""
Track ingested chunk content hashes for dedup and skip unchanged files on re-run.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ml.rag.paths import ingest_manifest_path
from ml.rag.text_processors.chunking_config import INGEST_VERSION


def default_manifest_path() -> Path:
    return ingest_manifest_path()


def load_manifest(path: Path | None = None) -> dict[str, Any]:
    p = path or default_manifest_path()
    if not p.exists():
        return {"version": INGEST_VERSION, "documents": {}, "content_hashes": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("documents", {})
            data.setdefault("content_hashes", {})
            return data
    except Exception:
        pass
    return {"version": INGEST_VERSION, "documents": {}, "content_hashes": {}}


def save_manifest(data: dict[str, Any], path: Path | None = None) -> None:
    p = path or default_manifest_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2), encoding="utf-8")


def should_skip_chunk(
    manifest: dict[str, Any],
    *,
    content_hash: str,
    document_id: str,
) -> bool:
    seen = manifest.get("content_hashes") or {}
    if content_hash in seen:
        return True
    doc = (manifest.get("documents") or {}).get(document_id) or {}
    if doc.get("ingest_version") == INGEST_VERSION and content_hash in (doc.get("hashes") or []):
        return True
    return False


def record_chunk(
    manifest: dict[str, Any],
    *,
    document_id: str,
    content_hash: str,
    source_file: str,
) -> None:
    manifest.setdefault("content_hashes", {})[content_hash] = {
        "document_id": document_id,
        "source_file": source_file,
    }
    docs = manifest.setdefault("documents", {})
    entry = docs.setdefault(document_id, {"hashes": [], "ingest_version": INGEST_VERSION})
    if content_hash not in entry["hashes"]:
        entry["hashes"].append(content_hash)
    entry["source_file"] = source_file
    entry["ingest_version"] = INGEST_VERSION

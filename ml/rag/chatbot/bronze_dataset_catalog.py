"""
Load bronze table/column catalog from ml/rag/bronze_dataset_model.yml (dbt-style fragment).

The file may be a YAML fragment starting with ``  - name: bronze``; we prepend ``sources:`` so
``yaml.safe_load`` returns ``{ "sources": [ { "name": "bronze", "tables": [...] } ] }``.

Used by bq_table_matcher to enrich vector-retrieved BQ description chunks with structured columns.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

_DEFAULT_YAML = Path(__file__).resolve().parent / "bronze_dataset_model.yml"
# Repo root: .../ml/rag/chatbot/this_file.py -> parents[3] == workspace root (e.g. data-team)
_REPO_ROOT = Path(__file__).resolve().parents[3]
_FALLBACK_DBT_SOURCES = _REPO_ROOT / "dbt" / "models" / "sources.yml"

# In-process cache: (cache_key_tuple, table_name -> compact schema string)
_cache: tuple[tuple[Any, ...], dict[str, str]] | None = None


def _default_yaml_path() -> Path:
    raw = os.environ.get("RAG_BRONZE_MODEL_YAML", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _DEFAULT_YAML.resolve()


def _source_filter_name() -> str | None:
    """Only load tables from this dbt source name (default bronze). Set empty to merge all sources."""
    raw = os.environ.get("RAG_BRONZE_MODEL_SOURCE", "bronze").strip()
    return raw if raw else None


def _format_table_columns(
    table: dict[str, Any],
    *,
    max_columns: int = 40,
    max_chars: int = 3000,
) -> str:
    cols = table.get("columns")
    if not isinstance(cols, list):
        return ""
    parts: list[str] = []
    total = 0
    for col in cols[:max_columns]:
        if not isinstance(col, dict):
            continue
        name = col.get("name")
        if not name:
            continue
        desc = str(col.get("description") or "").strip()
        safe = str(name).replace("`", "'")
        piece = f"`{safe}` {desc}".strip() if desc else f"`{safe}`"
        sep = 2
        if total + len(piece) + sep > max_chars:
            break
        parts.append(piece)
        total += len(piece) + sep
    return ", ".join(parts)


def _parse_loaded_yaml(data: Any, *, source_name: str | None) -> dict[str, str]:
    if not isinstance(data, dict):
        return {}
    sources = data.get("sources")
    if not isinstance(sources, list):
        return {}

    out: dict[str, str] = {}
    for src in sources:
        if not isinstance(src, dict):
            continue
        if source_name is not None and str(src.get("name") or "") != source_name:
            continue
        tables = src.get("tables")
        if not isinstance(tables, list):
            continue
        for t in tables:
            if not isinstance(t, dict):
                continue
            tid = t.get("name")
            if not tid or not isinstance(tid, str):
                continue
            schema = _format_table_columns(t)
            if schema:
                out[tid.strip()] = schema
    return out


def _parse_sources_text(text: str, *, source_name: str | None) -> dict[str, str]:
    stripped = text.strip()
    if not stripped:
        return {}
    if not stripped.startswith("sources:") and not stripped.startswith("version:"):
        doc = "sources:\n" + text
    else:
        doc = text
    try:
        data = yaml.safe_load(doc)
    except yaml.YAMLError:
        return {}
    return _parse_loaded_yaml(data, source_name=source_name)


def load_bronze_table_schemas(
    yaml_path: Path | None = None,
    *,
    force_reload: bool = False,
) -> dict[str, str]:
    """
    Return mapping table_id -> compact column list string for NL-to-SQL hints.

    Reloads when the file mtime changes or ``force_reload`` is True.

    Loads ``RAG_BRONZE_MODEL_YAML`` or ``ml/rag/bronze_dataset_model.yml`` (dbt fragment
    or full ``sources.yml``). If that file is missing, empty, or parses to zero tables,
    falls back to ``dbt/models/sources.yml`` (bronze source only unless
    ``RAG_BRONZE_MODEL_SOURCE`` is set).
    """
    global _cache
    path = yaml_path if yaml_path is not None else _default_yaml_path()
    source_name = _source_filter_name()
    try:
        mtime_primary = path.stat().st_mtime_ns if path.exists() else None
    except OSError:
        mtime_primary = None
    try:
        mtime_fb = _FALLBACK_DBT_SOURCES.stat().st_mtime_ns
    except OSError:
        mtime_fb = None

    cache_key = ("v1", str(path), mtime_primary, str(_FALLBACK_DBT_SOURCES), mtime_fb, source_name)
    if not force_reload and _cache is not None and _cache[0] == cache_key:
        return _cache[1]

    mapping: dict[str, str] = {}

    try:
        text = path.read_text(encoding="utf-8") if path.exists() else ""
    except OSError:
        text = ""
    if text.strip():
        mapping = _parse_sources_text(text, source_name=source_name)

    if not mapping and _FALLBACK_DBT_SOURCES.is_file():
        try:
            fb_text = _FALLBACK_DBT_SOURCES.read_text(encoding="utf-8")
        except OSError:
            fb_text = ""
        if fb_text.strip():
            mapping = _parse_sources_text(fb_text, source_name=source_name)

    _cache = (cache_key, mapping)
    return mapping

"""Per-table YAML loader for BigQuery semantic schemas under ``ml/rag/bq_tables_yaml_files``.

Each YAML carries SQL-relevant context the flat ``bronze_dataset_model.yml`` column
list does not have: grain, primary/foreign keys, join_logic, aggregation_rules,
filtering_guidance, sql_generation_hints, and columns annotated with semantic_role
and example values. This module loads the files once (mtime-cached) and exposes a
compact formatter suitable for injection into the NL-to-SQL prompt as ``table_hints``.

Public API:
- ``load_table_schema(name)``       -> raw dict for the table, or None.
- ``format_table_schema(name, ...)`` -> compact SQL-prompt block string, or "".
- ``known_table_names()``           -> set[str] of all indexed names (bare + FQN).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

# bq_table_schema_yaml.py lives at ml/rag/chatbot/, YAMLs live at ml/rag/bq_tables_yaml_files/.
_DEFAULT_DIR = Path(__file__).resolve().parents[1] / "bq_tables_yaml_files"

# Cache: (cache_key, index)
_cache: tuple[tuple[Any, ...], dict[str, dict[str, Any]]] | None = None


def _yaml_dir() -> Path:
    raw = os.environ.get("RAG_BQ_TABLES_YAML_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _DEFAULT_DIR.resolve()


def _strip_fqn(table_name: str) -> str:
    """Return the bare table name (last dotted segment) from a possibly fully-qualified id."""
    text = (table_name or "").strip().strip("`")
    if not text:
        return ""
    return text.split(".")[-1]


def _index_yaml_files(directory: Path) -> dict[str, dict[str, Any]]:
    """Build name -> table_schema_dict index, keyed by both bare and fully-qualified names."""
    out: dict[str, dict[str, Any]] = {}
    if not directory.is_dir():
        return out
    for path in directory.glob("*.yml"):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError):
            continue
        if not isinstance(data, dict):
            continue
        bare = path.stem
        # File-name key is authoritative; the explicit table_name field (often a FQN) is an alias.
        out[bare] = data
        declared = str(data.get("table_name") or "").strip().strip("`")
        if declared:
            out[declared] = data
            short = _strip_fqn(declared)
            if short and short != bare:
                out[short] = data
    return out


def _build_index() -> dict[str, dict[str, Any]]:
    """Load all YAML files; cache by (dir_path, dir_mtime, sorted file mtimes)."""
    global _cache
    directory = _yaml_dir()
    try:
        dir_mtime = directory.stat().st_mtime_ns if directory.is_dir() else None
    except OSError:
        dir_mtime = None
    file_sig: tuple[tuple[str, int], ...] = tuple()
    if dir_mtime is not None and directory.is_dir():
        try:
            file_sig = tuple(
                sorted(
                    (p.name, p.stat().st_mtime_ns)
                    for p in directory.glob("*.yml")
                )
            )
        except OSError:
            file_sig = tuple()
    cache_key = ("v1", str(directory), dir_mtime, file_sig)
    if _cache is not None and _cache[0] == cache_key:
        return _cache[1]
    index = _index_yaml_files(directory)
    _cache = (cache_key, index)
    return index


def known_table_names() -> set[str]:
    """All names (bare + FQN aliases) for which a YAML schema is available."""
    return set(_build_index().keys())


def load_table_schema(table_name: str) -> dict[str, Any] | None:
    """Resolve a table name to its raw YAML dict (trying FQN, bare, file-stem)."""
    name = (table_name or "").strip()
    if not name:
        return None
    index = _build_index()
    if name in index:
        return index[name]
    bare = _strip_fqn(name)
    if bare and bare in index:
        return index[bare]
    return None


# --- formatting -------------------------------------------------------------

_MAX_LINE = 140
_MAX_COLUMNS = 30


def _truncate(text: str, limit: int) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _format_list_field(label: str, value: Any) -> str | None:
    """Render scalar/list/dict YAML node as a single compact line."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        text = str(value).strip()
        if not text:
            return None
        return f"{label}: {_truncate(text, _MAX_LINE)}"
    if isinstance(value, list):
        flat: list[str] = []
        for item in value:
            if isinstance(item, (str, int, float, bool)):
                flat.append(str(item).strip())
            elif isinstance(item, dict):
                pair = next(iter(item.items()), None)
                if pair is not None:
                    k, v = pair
                    flat.append(f"{k}={_truncate(str(v), 60)}")
            if not flat:
                continue
        if not flat:
            return None
        return f"{label}: " + _truncate(", ".join(filter(None, flat)), _MAX_LINE)
    if isinstance(value, dict):
        bits: list[str] = []
        for k, v in value.items():
            if isinstance(v, (str, int, float, bool)):
                bits.append(f"{k}={_truncate(str(v), 60)}")
            elif isinstance(v, list):
                inner = ", ".join(str(x) for x in v if isinstance(x, (str, int, float, bool)))
                if inner:
                    bits.append(f"{k}=[{_truncate(inner, 80)}]")
        if not bits:
            return None
        return f"{label}: " + _truncate("; ".join(bits), _MAX_LINE)
    return None


def _format_columns(columns: Any, *, max_columns: int = _MAX_COLUMNS) -> str:
    """Render a YAML columns list as `name (type, role): description` lines."""
    if not isinstance(columns, list):
        return ""
    lines: list[str] = []
    for col in columns[:max_columns]:
        if not isinstance(col, dict):
            continue
        name = str(col.get("name") or "").strip()
        if not name:
            continue
        typ = str(col.get("type") or "").strip()
        role = str(col.get("semantic_role") or "").strip()
        desc = str(col.get("description") or "").strip()
        example = col.get("example")
        head = name
        meta_bits = []
        if typ:
            meta_bits.append(typ)
        if role:
            meta_bits.append(role)
        if meta_bits:
            head = f"{name} ({', '.join(meta_bits)})"
        tail = desc
        if example not in (None, ""):
            ex = _truncate(str(example), 40)
            tail = f"{tail} [ex: {ex}]" if tail else f"[ex: {ex}]"
        line = f"  - {head}" + (f": {_truncate(tail, _MAX_LINE - len(head) - 4)}" if tail else "")
        lines.append(line)
    if isinstance(columns, list) and len(columns) > max_columns:
        lines.append(f"  - … {len(columns) - max_columns} more columns")
    return "\n".join(lines)


# Section ordering optimized for NL-to-SQL prompt usefulness.
_SECTION_ORDER: list[tuple[str, str]] = [
    ("description", "Description"),
    ("grain", "Grain"),
    ("primary_keys", "Primary keys"),
    ("relationships", "Relationships"),
    ("join_logic", "Join logic"),
    ("time_dimensions", "Time dimensions"),
    ("geography", "Geography columns"),
    ("metrics", "Metric columns"),
    ("scenario_context", "Scenario context"),
    ("semantic_role", "Semantic role"),
    ("business_questions_supported", "Business questions supported"),
    ("aggregation_rules", "Aggregation rules"),
    ("filtering_guidance", "Filtering guidance"),
    ("sql_generation_hints", "SQL generation hints"),
    ("data_quality", "Data quality"),
    ("temporal_model", "Temporal model"),
]


def format_table_schema(
    table_name: str,
    *,
    max_chars: int = 2400,
    include_columns: bool = True,
) -> str:
    """Compact, SQL-prompt-friendly rendering of a per-table YAML schema.

    Returns "" when no YAML is known for the table. Output is bounded by
    ``max_chars`` to keep the NL-to-SQL prompt within the LM Studio context.
    """
    schema = load_table_schema(table_name)
    if not schema:
        return ""

    fqn = str(schema.get("table_name") or table_name).strip().strip("`")
    header = f"Table: {fqn or table_name}"
    parts: list[str] = [header]

    for key, label in _SECTION_ORDER:
        if key not in schema:
            continue
        line = _format_list_field(label, schema[key])
        if line:
            parts.append(line)

    if include_columns and isinstance(schema.get("columns"), list):
        col_block = _format_columns(schema["columns"])
        if col_block:
            parts.append("Columns:")
            parts.append(col_block)

    text = "\n".join(parts)
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)].rstrip() + "…"

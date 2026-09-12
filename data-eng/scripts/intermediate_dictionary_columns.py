"""Scan intermediate_dev SQL + schema.yml; build entities and column rows."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from intermediate_dictionary_data import (
    COLUMN_OVERRIDES,
    COMMON_COLUMN_DESC,
    ENTITY_OVERLAYS,
    generate_column_description,
)

ROOT = Path(__file__).resolve().parents[1]
INT_ROOT = ROOT / "dbt" / "models" / "intermediate_dev"
STG_ROOT = ROOT / "dbt" / "models" / "staging_dev"
SCHEMA_YML = INT_ROOT / "schema.yml"

NOISE_COLS = {
    "string",
    "int64",
    "float64",
    "bool",
    "boolean",
    "date",
    "timestamp",
    "numeric",
    "bytes",
    "array",
    "struct",
    "x",
    "d",
    "true",
    "false",
    "null",
}

COLUMN_HEADERS = [
    "table_name",
    "column_name",
    "data_type",
    "role",
    "description",
    "example",
]

REF_RE = re.compile(r"""\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}""")
SOURCE_RE = re.compile(
    r"""\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"""
)
SELECT_STAR_RE = re.compile(r"\bselect\s+\*\s+from\b", re.I)
SELECT_STAR_ALIAS_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\.\*", re.I)


def _strip_sql_noise(text: str) -> str:
    text_nc = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text_nc = re.sub(r"--.*?$", "", text_nc, flags=re.M)
    text_nc = re.sub(r"\{#.*?#\}", "", text_nc, flags=re.S)
    return text_nc


def cols_from_sql(text: str) -> list[str]:
    text_nc = _strip_sql_noise(text)
    found: list[str] = []
    for m in re.finditer(r"\bas\s+([A-Za-z_][A-Za-z0-9_]*)", text_nc, re.I):
        name = m.group(1)
        if name.lower() in NOISE_COLS:
            continue
        found.append(name)

    # Also parse the last SELECT … FROM projection for bare column names
    # (common in intermediate pass-through / conformed models).
    select_blocks = list(re.finditer(r"\bselect\b(.*?)\bfrom\b", text_nc, re.I | re.S))
    if select_blocks:
        body = select_blocks[-1].group(1)
        body_flat = re.sub(r"\bcase\b.*?\bend\b", " __case__ ", body, flags=re.I | re.S)
        for part in body_flat.split(","):
            part = part.strip()
            if not part or part == "__case__":
                continue
            as_m = re.search(r"\bas\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", part, re.I)
            if as_m:
                name = as_m.group(1)
                if name.lower() not in NOISE_COLS:
                    found.append(name)
                continue
            bare = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*$", part)
            if bare:
                name = bare.group(1)
                if name.lower() not in NOISE_COLS:
                    found.append(name)
                continue
            dotted = re.match(r"^[A-Za-z_][A-Za-z0-9_]*\.([A-Za-z_][A-Za-z0-9_]*)\s*$", part)
            if dotted:
                name = dotted.group(1)
                if name.lower() not in NOISE_COLS:
                    found.append(name)
    return list(dict.fromkeys(found))


def refs_from_sql(text: str) -> list[str]:
    out: list[str] = []
    for m in REF_RE.finditer(text):
        out.append(m.group(1))
    for m in SOURCE_RE.finditer(text):
        out.append(f"{m.group(1)}.{m.group(2)}")
    return list(dict.fromkeys(out))


def primary_upstream(refs: list[str], table_name: str) -> str:
    """Prefer staging/source feed; else first non-self int_ ref."""
    stg = [r for r in refs if r.startswith("stg_") or "." in r]
    if stg:
        return ", ".join(stg[:3])
    others = [r for r in refs if r != table_name]
    if others:
        return ", ".join(others[:3])
    return ""


def load_schema_models() -> dict[str, dict[str, Any]]:
    if not SCHEMA_YML.is_file():
        return {}
    data = yaml.safe_load(SCHEMA_YML.read_text(encoding="utf-8")) or {}
    out: dict[str, dict[str, Any]] = {}
    for model in data.get("models") or []:
        if not isinstance(model, dict):
            continue
        name = str(model.get("name") or "").strip()
        if not name:
            continue
        col_desc: dict[str, str] = {}
        for col in model.get("columns") or []:
            if not isinstance(col, dict):
                continue
            cname = str(col.get("name") or "").strip()
            cdesc = str(col.get("description") or "").strip()
            if cname and cdesc:
                col_desc[cname] = cdesc
        out[name] = {
            "description": str(model.get("description") or "").strip(),
            "column_descriptions": col_desc,
            "columns": [str(c.get("name") or "") for c in (model.get("columns") or []) if isinstance(c, dict)],
        }
    return out


def _find_sql(name: str) -> Path | None:
    if name.startswith("int_"):
        matches = list(INT_ROOT.rglob(f"{name}.sql"))
        return matches[0] if matches else None
    if name.startswith("stg_"):
        matches = list(STG_ROOT.rglob(f"{name}.sql"))
        return matches[0] if matches else None
    return None


def resolve_columns(table_name: str, text: str, *, _stack: frozenset[str] | None = None) -> list[str]:
    """Alias/bare columns; expand select * / alias.* from upstream int_/stg_ models."""
    stack = _stack or frozenset()
    if table_name in stack:
        return []
    cols = cols_from_sql(text)
    text_nc = _strip_sql_noise(text)
    has_star = bool(SELECT_STAR_RE.search(text_nc) or SELECT_STAR_ALIAS_RE.search(text_nc))
    if cols and not has_star:
        return cols

    # Map alias -> ref from FROM/JOIN clauses: from {{ ref('x') }} alias
    alias_to_ref: dict[str, str] = {}
    for m in re.finditer(
        r"""\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*)""",
        text,
        re.I,
    ):
        alias_to_ref[m.group(2)] = m.group(1)
    for m in re.finditer(
        r"""\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*)""",
        text,
        re.I,
    ):
        alias_to_ref[m.group(3)] = m.group(2)

    expand_refs: list[str] = []
    for m in SELECT_STAR_ALIAS_RE.finditer(text_nc):
        alias = m.group(1)
        if alias in alias_to_ref:
            expand_refs.append(alias_to_ref[alias])
    if SELECT_STAR_RE.search(text_nc):
        expand_refs.extend(refs_from_sql(text))

    for ref in list(dict.fromkeys(expand_refs + refs_from_sql(text))):
        bare = ref.split(".")[-1]
        if bare == table_name or bare in stack:
            continue
        path = _find_sql(bare)
        if path is None:
            continue
        upstream_text = path.read_text(encoding="utf-8", errors="replace")
        upstream_cols = resolve_columns(bare, upstream_text, _stack=stack | {table_name})
        if upstream_cols:
            # Local projected cols (geo_key etc.) win for ordering after upstream *
            local_only = [c for c in cols if c not in upstream_cols]
            return list(dict.fromkeys([*upstream_cols, *local_only]))
    return cols


def infer_role(col: str, pk: str) -> str:
    if col == pk or (pk and col == pk.split("(")[0].strip()):
        return "PK"
    if col in COMMON_COLUMN_DESC:
        return str(COMMON_COLUMN_DESC[col].get("role") or "dim")
    if col.endswith("_key"):
        return "FK"
    if col in {"value", "price_value", "population", "gdp_per_capita_ppp", "hdi"}:
        return "measure"
    if col in {"loaded_at", "profiled_at"}:
        return "meta"
    if col in {"tier", "data_level", "default_data_level", "producer_scale"}:
        return "acf"
    return "dim"


def scan_sql_models() -> dict[str, dict[str, Any]]:
    models: dict[str, dict[str, Any]] = {}
    for path in sorted(INT_ROOT.rglob("int_*.sql")):
        text = path.read_text(encoding="utf-8", errors="replace")
        refs = refs_from_sql(text)
        cols = resolve_columns(path.stem, text)
        models[path.stem] = {
            "domain": path.parent.name,
            "path": str(path.relative_to(ROOT)).replace("\\", "/"),
            "columns": cols,
            "upstream_refs": refs,
            "upstream": primary_upstream(refs, path.stem),
            "select_star": bool(SELECT_STAR_RE.search(_strip_sql_noise(text))),
        }
    return models


def build_entities(sql_models: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    schema = load_schema_models()
    models = sql_models or scan_sql_models()
    entities: list[dict[str, Any]] = []
    for name, info in sorted(models.items(), key=lambda kv: (kv[1]["domain"], kv[0])):
        sch = schema.get(name, {})
        overlay = ENTITY_OVERLAYS.get(name, {})
        purpose = str(overlay.get("purpose") or sch.get("description") or "").strip()
        if not purpose:
            purpose = (
                f"Intermediate {info['domain']} model {name}: conformed OpenTrace entity "
                f"feeding mart_dev from {info.get('upstream') or 'upstream staging/int sources'}."
            )
        entity: dict[str, Any] = {
            "table_name": name,
            "layer": "intermediate",
            "domain": info["domain"],
            "grain": str(overlay.get("grain") or "See SQL / schema tests for uniqueness"),
            "purpose": purpose,
            "primary_key": str(overlay.get("primary_key") or ""),
            "upstream": info.get("upstream") or "",
            "upstream_refs": list(info.get("upstream_refs") or []),
            "joins": str(overlay.get("joins") or ""),
            "path": info.get("path") or "",
        }
        entities.append(entity)
    return entities


def describe_column(
    *,
    table_name: str,
    column_name: str,
    domain: str,
    table_purpose: str,
    pk: str,
    schema_col_desc: str = "",
) -> tuple[str, str, str, str]:
    """Return role, description, example, data_type — description never blank."""
    over = (COLUMN_OVERRIDES.get(table_name) or {}).get(column_name, {})
    common = COMMON_COLUMN_DESC.get(column_name, {})
    role = str(over.get("role") or common.get("role") or infer_role(column_name, pk))
    desc = str(
        over.get("description")
        or schema_col_desc
        or common.get("description")
        or ""
    ).strip()
    if not desc:
        desc = generate_column_description(
            table_name=table_name,
            column_name=column_name,
            domain=domain,
            table_purpose=table_purpose,
            role=role,
        )
    example = str(over.get("example") or common.get("example") or "")
    dtype = str(over.get("data_type") or common.get("data_type") or "")
    return role, desc, example, dtype


def collect_column_rows(
    entities: list[dict[str, Any]] | None = None,
    sql_models: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[str], list[list[Any]], dict[str, dict[str, Any]]]:
    models = sql_models or scan_sql_models()
    entity_list = entities if entities is not None else build_entities(models)
    entities_by_name = {e["table_name"]: e for e in entity_list}
    schema = load_schema_models()

    col_rows: list[list[Any]] = []
    for name, info in sorted(models.items()):
        meta = entities_by_name.get(name, {})
        pk = str(meta.get("primary_key") or "")
        purpose = str(meta.get("purpose") or "")
        domain = str(meta.get("domain") or info.get("domain") or "")
        sch_cols = (schema.get(name) or {}).get("column_descriptions") or {}
        table_over = COLUMN_OVERRIDES.get(name, {})
        cols = list(info.get("columns") or [])
        for override_col in table_over:
            if override_col not in cols:
                cols.append(override_col)
        # Include schema-only tested columns
        for sc in (schema.get(name) or {}).get("columns") or []:
            if sc and sc not in cols:
                cols.append(sc)
        for col in cols:
            col_s = str(col)
            role, desc, example, dtype = describe_column(
                table_name=name,
                column_name=col_s,
                domain=domain,
                table_purpose=purpose,
                pk=pk,
                schema_col_desc=str(sch_cols.get(col_s) or ""),
            )
            col_rows.append([name, col_s, dtype, role, desc, example])
    return COLUMN_HEADERS, col_rows, models


def parse_relationships(entities: list[dict[str, Any]]) -> list[list[str]]:
    """Heuristic + schema relationship rows."""
    rows: list[list[str]] = []
    # schema.yml relationships if any
    if SCHEMA_YML.is_file():
        data = yaml.safe_load(SCHEMA_YML.read_text(encoding="utf-8")) or {}
        for model in data.get("models") or []:
            from_table = str(model.get("name") or "")
            for col in model.get("columns") or []:
                if not isinstance(col, dict):
                    continue
                col_name = str(col.get("name") or "")
                for test in col.get("tests") or []:
                    if isinstance(test, dict) and "relationships" in test:
                        rel = test["relationships"] or {}
                        rows.append(
                            [
                                from_table,
                                col_name,
                                str(rel.get("to") or ""),
                                str(rel.get("field") or ""),
                                "schema.yml relationships",
                            ]
                        )
    # Heuristic geo_key / source_natural_key
    for e in entities:
        name = e["table_name"]
        if name == "int_geography_conformed":
            continue
        # Detect from column inventory later; add generic notes from upstream
        if "with_geo" in name or name.endswith("_conformed"):
            rows.append(
                [
                    name,
                    "geo_key",
                    "int_geography_conformed",
                    "geo_key",
                    "heuristic: with_geo / conformed facts attach geography spine",
                ]
            )
            rows.append(
                [
                    name,
                    "source_natural_key",
                    "int_source_registry",
                    "source_natural_key",
                    "heuristic: lineage key for ACF source registry",
                ]
            )
    # Dedupe
    seen: set[tuple[str, str, str, str]] = set()
    out: list[list[str]] = []
    for r in rows:
        key = (r[0], r[1], r[2], r[3])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


__all__ = [
    "COLUMN_HEADERS",
    "INT_ROOT",
    "build_entities",
    "collect_column_rows",
    "load_schema_models",
    "parse_relationships",
    "primary_upstream",
    "scan_sql_models",
]

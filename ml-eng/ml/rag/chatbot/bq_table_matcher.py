"""
Match user queries to BigQuery table description chunks in the vector DB, then enrich
with structured per-table schemas (``ml/rag/bq_tables_yaml_files/*.yml``) and the
flat column catalog (``bronze_dataset_model.yml`` / dbt sources fallback) so each
fused hint carries semantic context the NL-to-SQL prompt can actually use:
grain, keys, joins, aggregation rules, filtering guidance, sql_generation_hints.

Output is one item per distinct table, ordered by best vector score, with ``content`` suitable
for ``BQRetriever`` ``table_hints``.
"""
from __future__ import annotations

import os
import re
from typing import Any

from ml.rag.chatbot.bq_table_schema_yaml import format_table_schema
from ml.rag.chatbot.bronze_dataset_catalog import load_bronze_table_schemas
from ml.rag.retrievers.vector_retriever import VectorRetriever

# metadata.type like "BQ yield_raw_data description"
_TYPE_TABLE_RE = re.compile(r"^BQ\s+(\S+)\s+description", re.IGNORECASE)
_TABLE_NAME_LINE_RE = re.compile(
    r"^\s*Table\s+Name:\s*(\S+)\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def _table_name_from_item(item: dict[str, Any]) -> str:
    md = item.get("metadata")
    meta: dict[str, Any]
    if isinstance(md, dict):
        meta = md
    else:
        meta = {}
    tn = str(meta.get("table_name") or "").strip()
    if tn:
        return tn
    typ = str(meta.get("type") or "").strip()
    m = _TYPE_TABLE_RE.match(typ)
    if m:
        return m.group(1).strip()
    text = str(item.get("content") or "")
    m2 = _TABLE_NAME_LINE_RE.search(text)
    if m2:
        return m2.group(1).strip()
    return ""


def _catalog_schema_for_table(
    catalog: dict[str, str],
    table_name: str,
    meta: dict[str, Any],
) -> str | None:
    schema = catalog.get(table_name) or catalog.get(table_name.strip())
    if schema:
        return schema
    bq_id = str(meta.get("bq_table_id") or "").strip()
    if bq_id:
        short = bq_id.split(".")[-1]
        return catalog.get(short) or catalog.get(short.strip())
    return None


def _build_fused_content(
    table_name: str,
    narrative: str,
    second_excerpt: str,
    catalog_schema: str | None,
    *,
    rich_schema: str | None = None,
) -> str:
    """Compose the SQL-prompt block for one table.

    Order chosen so the most-actionable signal (rich per-table YAML schema)
    sits closest to the question in the final prompt. ``catalog_schema``
    (flat column list) is included only as a column-naming fallback when
    the rich YAML block is unavailable.
    """
    parts = [f"Bronze table: {table_name}"]
    if rich_schema and rich_schema.strip():
        parts.append("Schema (semantic):")
        parts.append(rich_schema.strip())
    elif catalog_schema:
        parts.append(f"Catalog (columns): {catalog_schema}")
    else:
        parts.append("Catalog (columns): (not listed — fall back to BigQuery live schema).")
    ne = narrative.strip()
    if second_excerpt.strip():
        ne = f"{ne}\n\n{second_excerpt.strip()}"
    parts.append(f"Description excerpts: {ne}")
    return "\n".join(parts)


def match_bq_tables_from_descriptions(
    query: str,
    top_k: int = 8,
    collection_name: str | None = None,
) -> list[dict[str, Any]]:
    """
    Retrieve BQ description chunks from Qdrant, group by table, merge YAML/dbt catalog columns
    with narrative text. Returns one dict per table (``content`` fused for NL-to-SQL hints).
    """
    coll = (
        collection_name
        or os.environ.get("QDRANT_COLLECTION_DATA_DESCRIPTIONS", "BQ_table_descriptions").strip()
        or "BQ_table_descriptions"
    )
    vr = VectorRetriever(collection_name=coll)
    raw = vr.retrieve(
        query,
        top_k=top_k,
        doc_kind="bq_table_description",
        vector_search_mode="bq_triple",
    )
    catalog = load_bronze_table_schemas()

    # table_name -> list of (score, content, meta)
    groups: dict[str, list[tuple[float, str, dict[str, Any]]]] = {}
    metas_by_table: dict[str, dict[str, Any]] = {}
    for item in raw:
        tn = _table_name_from_item(item)
        if not tn:
            continue
        score = float(item.get("score") or 0.0)
        content = str(item.get("content") or "").strip()
        raw_md = item.get("metadata")
        md: dict[str, Any] = raw_md if isinstance(raw_md, dict) else {}
        groups.setdefault(tn, []).append((score, content, md))
        if tn not in metas_by_table:
            metas_by_table[tn] = md

    out: list[dict[str, Any]] = []
    for tn, scored_chunks in groups.items():
        scored_chunks.sort(key=lambda x: x[0], reverse=True)
        best_score = scored_chunks[0][0]
        primary = scored_chunks[0][1]
        second = ""
        if len(scored_chunks) > 1 and scored_chunks[1][1]:
            second = scored_chunks[1][1][:400]
            if len(scored_chunks[1][1]) > 400:
                second += "…"

        schema = _catalog_schema_for_table(catalog, tn, metas_by_table.get(tn, {}))
        # Rich per-table YAML (grain, keys, joins, sql_generation_hints, etc.).
        rich = format_table_schema(tn)
        if not rich:
            # Fall back to the FQN if the bare-name lookup missed.
            bq_id = str(metas_by_table.get(tn, {}).get("bq_table_id") or "").strip()
            if bq_id:
                rich = format_table_schema(bq_id)
        fused = _build_fused_content(tn, primary, second, schema, rich_schema=rich)

        meta_base = {
            "table_name": tn,
            "catalog_matched": bool(schema),
            "rich_schema_matched": bool(rich),
        }
        out.append(
            {
                "content": fused,
                "score": best_score,
                "metadata": meta_base,
                "source": "bq_table_match",
            }
        )

    out.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)

    # If metadata never yielded a table id, pass through raw vector rows (legacy behavior).
    if not out and raw:
        return raw

    return out

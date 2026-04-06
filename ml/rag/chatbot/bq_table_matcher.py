"""
Match user queries to BigQuery table description chunks in the vector DB, then enrich
with structured column catalogs from bronze_dataset_model.yml (or dbt sources fallback).

Output is one item per distinct table, ordered by best vector score, with ``content`` suitable
for ``BQRetriever`` ``table_hints``.
"""
from __future__ import annotations

import re
from typing import Any

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


def _build_fused_content(
    table_name: str,
    narrative: str,
    second_excerpt: str,
    catalog_schema: str | None,
) -> str:
    parts = [f"Bronze table: {table_name}"]
    if catalog_schema:
        parts.append(f"Catalog (columns): {catalog_schema}")
    else:
        parts.append("Catalog (columns): (not listed in bronze dataset model — use live schema).")
    ne = narrative.strip()
    if second_excerpt.strip():
        ne = f"{ne}\n\n{second_excerpt.strip()}"
    parts.append(f"Description excerpts: {ne}")
    return "\n".join(parts)


def match_bq_tables_from_descriptions(
    query: str,
    top_k: int = 8,
    collection_name: str = "opentrace_rag",
) -> list[dict[str, Any]]:
    """
    Retrieve BQ description chunks from Chroma, group by table, merge YAML/dbt catalog columns
    with narrative text. Returns one dict per table (``content`` fused for NL-to-SQL hints).
    """
    vr = VectorRetriever(collection_name=collection_name)
    raw = vr.retrieve(
        query,
        top_k=top_k,
        doc_kind="bq_table_description",
    )
    catalog = load_bronze_table_schemas()

    # table_name -> list of (score, content)
    groups: dict[str, list[tuple[float, str]]] = {}
    for item in raw:
        tn = _table_name_from_item(item)
        if not tn:
            continue
        score = float(item.get("score") or 0.0)
        content = str(item.get("content") or "").strip()
        groups.setdefault(tn, []).append((score, content))

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

        schema = catalog.get(tn) or catalog.get(tn.strip())
        fused = _build_fused_content(tn, primary, second, schema)

        meta_base = {"table_name": tn, "catalog_matched": bool(schema)}
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

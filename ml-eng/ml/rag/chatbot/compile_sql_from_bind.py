"""Deterministic single-table SQL from TableBindContract (opt-in via RAG_BIND_SQL_COMPILER)."""
from __future__ import annotations

import os
from typing import Any

from ml.rag.chatbot.bq_mart_sql import mart_dataset
from ml.rag.chatbot.bq_table_schema_yaml import TableBindContract


def bind_sql_compiler_enabled() -> bool:
    return os.environ.get("RAG_BIND_SQL_COMPILER", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def compile_sql_from_bind(
    contract: TableBindContract | dict[str, Any],
    *,
    project_id: str | None = None,
    dataset: str | None = None,
    limit: int = 1,
) -> str | None:
    """Compile a point fact_lookup SELECT from bind contract filters."""
    if isinstance(contract, dict):
        parsed = TableBindContract.from_dict(contract)
        if parsed is None:
            return None
        contract = parsed

    if not str(contract.required_filters_sql or "").strip():
        return None

    cols = [c for c in contract.measure_columns if str(c).strip()]
    if not cols:
        return None

    proj = (project_id or os.environ.get("BQ_PROJECT", "opentrace-prod-5ga4")).strip()
    ds = (dataset or mart_dataset()).strip()
    bare = str(contract.table_id or "").strip().split(".")[-1].lower()
    if not bare:
        return None

    fqn = f"`{proj}.{ds}.{bare}`"
    select_cols = ", ".join(cols[:4])
    where_sql = str(contract.required_filters_sql or "").strip()
    if where_sql.upper().startswith("AND "):
        where_sql = where_sql[4:].strip()
    if not where_sql:
        return None

    cap = max(1, int(limit or 1))
    return f"SELECT {select_cols} FROM {fqn} WHERE {where_sql} LIMIT {cap}"


__all__ = ["bind_sql_compiler_enabled", "compile_sql_from_bind"]

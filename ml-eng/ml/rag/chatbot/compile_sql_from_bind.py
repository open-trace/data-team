"""Deterministic single-table SQL from TableBindContract for point fact_lookup."""
from __future__ import annotations

import os
from typing import Any

from ml.rag.chatbot.bq_mart_sql import mart_dataset
from ml.rag.chatbot.bq_table_schema_yaml import TableBindContract


def bind_contract_for_table(
    bind_contracts: dict[str, Any] | None,
    table_id: str,
) -> dict[str, Any] | None:
    """Resolve a bind map entry; table ids match on the bare, case-insensitive name."""
    if not isinstance(bind_contracts, dict) or not bind_contracts:
        return None
    needle = str(table_id or "").strip().split(".")[-1].lower()
    if not needle:
        return None
    raw = bind_contracts.get(table_id)
    if isinstance(raw, dict):
        return raw
    raw = bind_contracts.get(needle)
    if isinstance(raw, dict):
        return raw
    for key, value in bind_contracts.items():
        if str(key or "").strip().split(".")[-1].lower() == needle and isinstance(value, dict):
            return value
    return None


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


__all__ = ["bind_contract_for_table", "compile_sql_from_bind"]

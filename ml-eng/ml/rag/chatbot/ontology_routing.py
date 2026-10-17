"""Ontology-driven routing (opentrace_ontology v3). Offline, ~0.1 ms per question. All flags default OFF."""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

try:
    import opentrace_ontology as _oo
except Exception:  # pragma: no cover
    _oo = None


def _on(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "on", "yes")


def ontology_plan_for_state(state: Any) -> dict[str, Any] | None:
    """plan_query once per turn, cached on state['ontology_plan'] + state['source_basis']. None when disabled."""
    if _oo is None or not _on("ADZA_ONTOLOGY_ROUTING"):
        return None
    if isinstance(state, dict) and isinstance(state.get("ontology_plan"), dict):
        return state["ontology_plan"]
    q = str((state or {}).get("normalized_query") or (state or {}).get("query") or "").strip()
    if not q:
        return None
    try:
        plan = _oo.plan_query(q)
    except Exception:
        logger.warning("ontology plan failed", exc_info=True)
        return None
    if isinstance(state, dict):
        state["ontology_plan"] = plan
        state["source_basis"] = plan["source_basis"]
    return plan


def subject_entities(plan: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [
        e
        for e in (plan or {}).get("entities", [])
        if e.get("entity_id") and e.get("entity_type") != "country"
    ]


def qdrant_payload_ids(plan: dict[str, Any] | None) -> list[str]:
    ids: list[str] = []
    for e in subject_entities(plan):
        if e["entity_type"] in ("measure", "unit", "indicator_class") or str(
            e["entity_id"]
        ).startswith("OT:MV:"):
            continue
        ids.extend(e.get("payload_ids") or [])
    return list(dict.fromkeys(ids))[:64]


def qdrant_expansion_view(plan: dict[str, Any] | None, max_terms: int = 8) -> str | None:
    """One extra query view ONLY for subjects missing from BigQuery (documents are the only evidence)."""
    if not _on("ADZA_ONTOLOGY_QUERY_VIEW"):
        return None
    parts = [
        t
        for e in subject_entities(plan)
        if e.get("mode") == "qdrant_only"
        for t in (e.get("qdrant_terms") or [])[:max_terms]
    ]
    return " ".join(dict.fromkeys(parts)) or None


def bq_value_filters(plan: dict[str, Any] | None) -> dict[str, list[str]]:
    """'table.column' -> exact stored strings for entities in BigQuery (NG vs NGA vs 'Nigeria' per column)."""
    out: dict[str, list[str]] = {}
    for e in (plan or {}).get("entities", []):
        if e.get("mode") != "bq+qdrant":
            continue
        for col, vals in (e.get("bq_targets") or {}).items():
            cur = out.setdefault(col, [])
            cur.extend(v for v in vals if v not in cur)
    return out


def bq_should_run(plan: dict[str, Any] | None) -> bool:
    return True if plan is None else bool(plan.get("run_bigquery"))


def acf_band_ceiling(plan: dict[str, Any] | None, cited_kinds: set[str]) -> str | None:
    """No BQ rows cited -> documents-only answers cap at 'moderate'; web-only at 'limited'."""
    if cited_kinds & {"bigquery", "structured_data"}:
        return None
    if cited_kinds & {"vector", "qdrant"}:
        return "moderate"
    if "web" in cited_kinds:
        return "limited"
    return (plan or {}).get("acf_ceiling")

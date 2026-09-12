"""FS engine: food security IPC and household FIES."""
from __future__ import annotations

import re
from typing import Any

from ml.rag.chatbot.class_engines.base import ClassEngine, EngineResult
from ml.rag.chatbot.class_engines.shared import bind_value_hits, build_planned_multi_table_result
from ml.rag.chatbot.class_table_router import TablePlan, select_table_plans
from ml.rag.chatbot.intent_bundles import match_intent_bundles
from ml.rag.chatbot.schema_card import load_schema_card
from ml.rag.chatbot.value_index import resolve_geography_iso3

_IPC_POP_RE = re.compile(r"\b(ipc|phase\s*[345]|population|people|humanitarian)\b", re.I)


def _agg_coverage_countries(card: dict[str, Any]) -> set[str]:
    coverage = card.get("coverage") or {}
    agg = coverage.get("agg_food_security_monthly") or coverage.get("agg_food_security_country_month")
    if not isinstance(agg, dict):
        return set()
    raw = agg.get("country_iso3") or []
    return {str(c).strip().upper() for c in raw if str(c).strip()}


def _filter_fs_plans(
    plans: list[TablePlan],
    *,
    use_agg: bool,
    any_iso_covered: bool,
) -> list[TablePlan]:
    """Narrow router plans by agg coverage preference (no hard-coded bypass of router)."""
    if not plans:
        return plans
    if use_agg:
        agg_plans = [p for p in plans if "agg_food_security" in p.table_id]
        return agg_plans or plans[:1]
    out: list[TablePlan] = []
    for p in plans:
        if "agg_food_security" in p.table_id and not any_iso_covered:
            continue
        out.append(p)
    return out or plans[:1]


class FsEngine(ClassEngine):
    class_code = "FS"

    def run_plan(
        self,
        query: str,
        *,
        facets: dict[str, Any],
        card: dict[str, Any] | None = None,
    ) -> EngineResult:
        card = card or load_schema_card("FS") or {}
        bundles = match_intent_bundles(query, facets)
        geography = facets.get("geography") if isinstance(facets.get("geography"), list) else []
        expanded = facets.get("expanded_regions") if isinstance(facets.get("expanded_regions"), list) else None
        iso_list = resolve_geography_iso3(query, geography=geography, expanded_regions=expanded)
        if not iso_list:
            return EngineResult(
                class_code="FS",
                status="planner_error",
                table_id="",
                sql=None,
                caveats=["missing_geography"],
            )

        covered = _agg_coverage_countries(card)
        any_covered = any(str(c).upper() in covered for c in iso_list)
        use_agg = bool(
            iso_list
            and str(iso_list[0]).upper() in covered
            and _IPC_POP_RE.search(query)
        )

        plans = select_table_plans(
            "FS",
            query=query,
            facets=facets,
            bundles=bundles,
            card=card,
            iso_list=iso_list,
        )
        plans = _filter_fs_plans(plans, use_agg=use_agg, any_iso_covered=any_covered)
        if not plans:
            return EngineResult(
                class_code="FS",
                status="planner_error",
                table_id=str(card.get("default_table") or "fct_food_security"),
                sql=None,
                caveats=["no_table_plans"],
                value_hits={"country_iso3": iso_list},
            )

        hits = bind_value_hits(card, query=query, facets=facets)
        hits["country_iso3"] = iso_list

        return build_planned_multi_table_result(
            class_code="FS",
            plans=plans,
            query=query,
            facets=facets,
            card=card,
            value_hits=hits,
            iso_list=iso_list,
            measure_id="food_security",
        )


__all__ = ["FsEngine"]

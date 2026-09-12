"""Structured context and prompt assembly for model-first query decompose."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ml.rag.chatbot.graph import RAGGraphState

from ml.rag.chatbot.agri_entities import CROP_COMMODITY_TERMS
from ml.rag.chatbot.agri_measure_ontology import decompose_measure_vocabulary
from ml.rag.chatbot.geo_regions import decompose_region_vocabulary
from ml.rag.chatbot.plan_policy import allows_cross_country, plan_generation_addendum

# Stakeholder insight intents (mirrored in query_decomposer for heuristics).
INTENT_ALLOWED: tuple[str, ...] = (
    "descriptive",
    "diagnostic",
    "predictive",
    "monitoring",
    "compare",
    "locate",
    "decision_support",
)

INTENT_LLM_LINES: tuple[str, ...] = (
    "descriptive: what happened, levels, trends, summaries from data or documents",
    "diagnostic: why, drivers, contributing factors (ground claims carefully)",
    "predictive: forward-looking (forecast, likely, outlook, scenario) — note uncertainty",
    "monitoring: ongoing watch, worsening, emerging risks, what to track",
    "compare: versus, rank, best/worst, regional or group comparison",
    "locate: where to focus, priority areas, geographic targeting",
    "decision_support: planning, funding, sourcing, investment, policy choices",
)

JOB_ALLOWED: tuple[str, ...] = (
    "fact",
    "breakdown",
    "trend",
    "rank",
    "compare",
    "list",
    "outlook",
    "diagnose",
    "brief",
    "report",
    "synthesis",
)

JOB_LLM_LINES: tuple[str, ...] = (
    "fact: single value or level lookup",
    "breakdown: split by dimension (crop, region, sex, etc.)",
    "trend: change over time",
    "rank: order countries/regions by a metric",
    "compare: side-by-side between two or more geographies",
    "list: which items/countries meet a condition",
    "outlook: forward-looking food security or season outlook",
    "diagnose: explain drivers or causes",
    "brief: news-style summary or headlines",
    "report: multi-part analytical briefing",
    "synthesis: combine narrative and numeric evidence",
)

GEO_SCOPE_ALLOWED: tuple[str, ...] = (
    "country",
    "multi_country",
    "region",
    "continent",
    "unspecified",
)

DECOMPOSE_MEASURE_HINTS: tuple[str, ...] = (
    "production",
    "yield",
    "area_harvested",
    "trade",
    "market_price",
    "food_security_ipc",
    "food_balance",
    "rainfall",
    "climate",
    "population",
    "hdi",
    "gdp",
    "livestock",
    "soil",
)

CROP_ALIAS_NOTE = "Aliases: corn=maize, paddy=rice, soy=soybean, groundnuts=groundnut"


@dataclass
class DecomposeContext:
    """Session and plan context for the decompose LLM.

    ``query`` is always the real user_query. ``context_rewrite`` is optional
    related history/profile companion text for elliptical turns.
    """

    query: str
    original_query: str = ""
    context_rewrite: str = ""
    context_applied: bool = False
    conversation_summary: str = ""
    prior_topic: str | None = None
    memory_enriched: bool = False
    plan_type: str = ""
    category: str = ""
    profile_country: str = ""
    geo_override: str = ""
    time_start_override: str = ""
    time_end_override: str = ""
    today_iso: str = field(default_factory=lambda: date.today().isoformat())

    @classmethod
    def from_query(cls, query: str) -> DecomposeContext:
        q = (query or "").strip()
        return cls(query=q, original_query=q)

    @classmethod
    def from_graph_state(
        cls,
        state: RAGGraphState | dict[str, Any] | None,
        enrich: dict[str, Any] | None,
    ) -> DecomposeContext:
        enrich = enrich if isinstance(enrich, dict) else {}
        state = state if isinstance(state, dict) else {}
        raw_q = str(
            enrich.get("user_query")
            or enrich.get("original_query")
            or state.get("user_query")
            or state.get("query")
            or ""
        ).strip()
        context_rewrite = str(
            enrich.get("context_rewrite") or state.get("context_rewrite") or ""
        ).strip()
        context_applied = bool(
            enrich.get("context_applied")
            if "context_applied" in enrich
            else state.get("context_applied")
        )
        if not context_applied and enrich.get("enriched") and not context_rewrite:
            # Legacy enrich dict shape
            enriched_q = str(enrich.get("enriched_query") or "").strip()
            if enriched_q and enriched_q != raw_q:
                context_rewrite = enriched_q
                context_applied = True
        profile = state.get("user_profile") if isinstance(state.get("user_profile"), dict) else {}
        category = str(state.get("category") or (profile or {}).get("category") or "").strip()
        return cls(
            query=raw_q,
            original_query=raw_q,
            context_rewrite=context_rewrite if context_applied else "",
            context_applied=context_applied,
            conversation_summary=str(state.get("conversation_summary") or "").strip(),
            prior_topic=str(enrich.get("prior_topic") or "").strip() or None,
            memory_enriched=bool(enrich.get("enriched") or context_applied),
            plan_type=str(state.get("plan_type") or "").strip(),
            category=category,
            profile_country=str((profile or {}).get("country") or "").strip(),
            geo_override=str(state.get("geo_override") or "").strip(),
            time_start_override=str(state.get("time_start_override") or "").strip()[:10],
            time_end_override=str(state.get("time_end_override") or "").strip()[:10],
        )


def _crop_vocabulary_compact(max_terms: int = 40) -> str:
    terms = sorted(CROP_COMMODITY_TERMS, key=len)[:max_terms]
    return ", ".join(terms)


def format_decompose_system_prompt() -> str:
    intent_block = "\n".join(f"  - {line}" for line in INTENT_LLM_LINES)
    job_block = "\n".join(f"  - {line}" for line in JOB_LLM_LINES)
    intent_csv = ", ".join(INTENT_ALLOWED)
    job_csv = ", ".join(JOB_ALLOWED)
    geo_scope_csv = ", ".join(GEO_SCOPE_ALLOWED)
    measure_csv = ", ".join(DECOMPOSE_MEASURE_HINTS)
    return (
        "You are the query decomposer for OpenTrace Ask ADZA — Africa-first agricultural intelligence. "
        "Users are government, NGOs, agribusiness, finance, and rural communities. They rarely mention databases.\n\n"
        "Extract structured JSON for retrieval routing. Never mention SQL or table names.\n\n"
        "Multilingual: questions may be in English, French, Swahili, Hausa, Arabic, Portuguese, or code-mixed African English. "
        "Extract semantics and emit canonical English country and crop names in JSON.\n\n"
        "Africa semantics:\n"
        "- OpenTrace defaults to African agricultural intelligence.\n"
        "- For unscoped 'which country' ranking questions, set africa_default=true and leave geography empty.\n"
        "- For 'all African countries' / country-by-country panels, set africa_panel=true and leave geography empty.\n"
        "- Never put literal words 'country' or 'countries' in the geography array.\n"
        "- Region labels (West Africa, ECOWAS, SADC) belong in geography or entities when the user names them.\n\n"
        "Grounding:\n"
        "- Only emit geography and entities evidenced in the question (or conversation context when elliptical).\n"
        "- Crop alias normalization is allowed (e.g. user says paddy → entity rice).\n"
        "- Do not invent countries or crops not supported by the text.\n\n"
        f"intent must be EXACTLY one of: {intent_csv}\n{intent_block}\n\n"
        f"job must be EXACTLY one of: {job_csv}\n{job_block}\n\n"
        f"geo_scope must be EXACTLY one of: {geo_scope_csv}\n\n"
        f"primary_measure_hints: array of measure ids from this list (may be empty): {measure_csv}\n\n"
        "Return ONLY valid JSON with keys:\n"
        "  intent, job, entities, geography, domains, geo_scope,\n"
        "  primary_measure_hints, time_start, time_end,\n"
        "  africa_panel (boolean, optional), africa_default (boolean, optional)\n\n"
        "Use time_end = today's date when the question says till now, to date, until now, "
        "or since YEAR with no fixed end year. Use empty time_start/time_end when no time is implied.\n"
        "No markdown, no extra keys."
    )


def format_decompose_user_prompt(
    ctx: DecomposeContext,
    provisional: dict[str, Any] | None,
) -> str:
    blocks: list[str] = [f"Today's date: {ctx.today_iso}"]

    if ctx.plan_type:
        blocks.append(f"Plan tier: {ctx.plan_type}")
        addendum = plan_generation_addendum(ctx.plan_type)
        if addendum:
            blocks.append(addendum)
        if not allows_cross_country(ctx.plan_type):
            blocks.append(
                "Plan constraint: prefer a single country in geography unless the user explicitly names a region or multiple countries."
            )

    if ctx.category:
        blocks.append(f"Category lens: {ctx.category}")

    if ctx.profile_country:
        blocks.append(f"Profile country (default when elliptical): {ctx.profile_country}")

    ui_parts: list[str] = []
    if ctx.geo_override:
        ui_parts.append(f"geo={ctx.geo_override}")
    if ctx.time_start_override:
        ui_parts.append(f"time_start={ctx.time_start_override}")
    if ctx.time_end_override:
        ui_parts.append(f"time_end={ctx.time_end_override}")
    if ui_parts:
        blocks.append("UI scope (authoritative when set): " + "; ".join(ui_parts))

    if ctx.conversation_summary:
        blocks.append(f"Conversation summary:\n{ctx.conversation_summary[:1200]}")

    if ctx.prior_topic and ctx.context_applied:
        blocks.append(f"Prior user topic:\n{ctx.prior_topic[:800]}")

    blocks.append(f"Question: {ctx.query}")
    if ctx.context_applied and ctx.context_rewrite and ctx.context_rewrite != ctx.query:
        blocks.append(
            "Context rewrite (related history/profile; use for elliptical grounding only):\n"
            f"{ctx.context_rewrite[:1200]}"
        )

    if provisional:
        blocks.append(
            "Provisional heuristic hints (confirm or correct):\n"
            + json.dumps(provisional, ensure_ascii=False, indent=2)
        )

    blocks.append("Recognized region labels (use when user names a zone; do not expand to all countries):\n")
    blocks.append(decompose_region_vocabulary())
    blocks.append("Measure vocabulary (primary_measure_hints must use these ids):\n")
    blocks.append(decompose_measure_vocabulary())
    blocks.append(f"Crop vocabulary: {_crop_vocabulary_compact()}")
    blocks.append(CROP_ALIAS_NOTE)

    return "\n\n".join(blocks)


__all__ = [
    "CROP_ALIAS_NOTE",
    "DECOMPOSE_MEASURE_HINTS",
    "DecomposeContext",
    "GEO_SCOPE_ALLOWED",
    "INTENT_ALLOWED",
    "INTENT_LLM_LINES",
    "JOB_ALLOWED",
    "JOB_LLM_LINES",
    "format_decompose_system_prompt",
    "format_decompose_user_prompt",
]

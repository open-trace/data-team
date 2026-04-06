"""
Decompose a user query into semantic facets for retrieval routing and filtering.

Intent is stakeholder-oriented (descriptive, diagnostic, predictive, monitoring, compare,
locate, decision_support), not database- or channel-specific. Heuristics run by default;
optional LLM enrichment when HF_API_TOKEN is set. Output is JSON-serializable for graph state.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any

# Stakeholder-oriented insight intents (not DB/channel labels). Used in heuristics, LLM prompt, and normalization.
INTENT_ALLOWED: tuple[str, ...] = (
    "descriptive",
    "diagnostic",
    "predictive",
    "monitoring",
    "compare",
    "locate",
    "decision_support",
)

# One-line definitions for the LLM (no database vocabulary).
_INTENT_LLM_LINES = (
    "descriptive: what happened, levels, trends, summaries from data or documents",
    "diagnostic: why, drivers, contributing factors (ground claims carefully)",
    "predictive: forward-looking (forecast, likely, outlook, scenario) — note uncertainty",
    "monitoring: ongoing watch, worsening, emerging risks, what to track",
    "compare: versus, rank, best/worst, regional or group comparison",
    "locate: where to focus, priority areas, geographic targeting",
    "decision_support: planning, funding, sourcing, investment, policy choices",
)

_INTENT_ALIASES: dict[str, str] = {
    "general": "descriptive",
    "data_analytics": "descriptive",
    "schema_lookup": "descriptive",
    "news": "descriptive",
    "academic": "descriptive",
    "prediction": "predictive",
    "forecasting": "predictive",
    "comparison": "compare",
    "ranking": "compare",
    "monitor": "monitoring",
    "tracking": "monitoring",
    "diagnosis": "diagnostic",
    "causal": "diagnostic",
    "decision": "decision_support",
    "recommendation": "decision_support",
    "location": "locate",
    "prioritization": "locate",
}

# Common country / region tokens for agriculture news (extend as needed)
_COUNTRY_ALIASES = {
    # African countries and their common aliases - full list (as of 2023)
    "algeria": "Algeria",
    "angola": "Angola",
    "benin": "Benin",
    "botswana": "Botswana",
    "burkina faso": "Burkina Faso",
    "burundi": "Burundi",
    "cabo verde": "Cabo Verde",
    "cape verde": "Cabo Verde",
    "cameroon": "Cameroon",
    "central african republic": "Central African Republic",
    "car": "Central African Republic",
    "chad": "Chad",
    "comoros": "Comoros",
    "congo": "Republic of the Congo",
    "congo-brazzaville": "Republic of the Congo",
    "congo republic": "Republic of the Congo",
    "republic of the congo": "Republic of the Congo",
    "congo-kinshasa": "Democratic Republic of the Congo",
    "drc": "Democratic Republic of the Congo",
    "democratic republic of the congo": "Democratic Republic of the Congo",
    "djibouti": "Djibouti",
    "egypt": "Egypt",
    "equatorial guinea": "Equatorial Guinea",
    "eritrea": "Eritrea",
    "eswatini": "Eswatini",
    "swaziland": "Eswatini",
    "ethiopia": "Ethiopia",
    "gabon": "Gabon",
    "gambia": "Gambia",
    "ghana": "Ghana",
    "guinea": "Guinea",
    "guinea-bissau": "Guinea-Bissau",
    "ivory coast": "Côte d'Ivoire",
    "côte d'ivoire": "Côte d'Ivoire",
    "kenya": "Kenya",
    "lesotho": "Lesotho",
    "liberia": "Liberia",
    "libya": "Libya",
    "madagascar": "Madagascar",
    "malawi": "Malawi",
    "mali": "Mali",
    "mauritania": "Mauritania",
    "mauritius": "Mauritius",
    "morocco": "Morocco",
    "mozambique": "Mozambique",
    "namibia": "Namibia",
    "niger": "Niger",
    "nigeria": "Nigeria",
    "rwanda": "Rwanda",
    "sao tome and principe": "Sao Tome and Principe",
    "senegal": "Senegal",
    "seychelles": "Seychelles",
    "sierra leone": "Sierra Leone",
    "somalia": "Somalia",
    "south africa": "South Africa",
    "south sudan": "South Sudan",
    "sudan": "Sudan",
    "tanzania": "Tanzania",
    "togo": "Togo",
    "tunisia": "Tunisia",
    "uganda": "Uganda",
    "zambia": "Zambia",
    "zimbabwe": "Zimbabwe",

}

_DOMAIN_KEYWORDS = (
    "yield",
    "crop",
    "livestock",
    "food security",
    "drought",
    "rainfall",
    "climate",
    "soil",
    "fertilizer",
    "trade",
    "export",
    "import",
    "policy",
    "subsidy",
    "smallholder",
    "irrigation",
)


def _extract_countries(text: str) -> list[str]:
    q = text.lower()
    found: list[str] = []
    for key, canonical in sorted(_COUNTRY_ALIASES.items(), key=lambda x: -len(x[0])):
        if key in q:
            if canonical not in found:
                found.append(canonical)
    return found


def _extract_year_range(text: str) -> tuple[str | None, str | None]:
    """Return (start_iso, end_iso) as YYYY-MM-DD or (None, None)."""
    years = [int(m.group(0)) for m in re.finditer(r"\b(19|20)\d{2}\b", text)]
    if not years:
        return None, None
    years = sorted(set(years))
    if len(years) >= 2:
        y0, y1 = years[0], years[-1]
        return f"{y0}-01-01", f"{y1}-12-31"
    y = years[0]
    return f"{y}-01-01", f"{y}-12-31"


def _normalize_intent(value: str | None) -> str:
    """Map free-text or legacy intent to exactly one of INTENT_ALLOWED; default descriptive."""
    if not value or not str(value).strip():
        return "descriptive"
    raw = str(value).strip().lower().replace(" ", "_")
    if raw in INTENT_ALLOWED:
        return raw
    if raw in _INTENT_ALIASES:
        return _INTENT_ALIASES[raw]
    # Substring / fuzzy: e.g. "predictive_analysis"
    return "descriptive"


def _infer_intent(text: str) -> str:
    """
    Classify query by insight type (first match wins). Order: predictive → monitoring → compare
    → locate → decision_support → diagnostic → descriptive.
    """
    tl = text.lower()
    padded = f" {tl} "

    pred_kw = (
        "likely",
        "forecast",
        "predict",
        "projection",
        "outlook",
        "scenario",
        "what if",
        "expect",
    )
    if any(k in tl for k in pred_kw) or re.search(r"\bwill\b", tl):
        return "predictive"

    mon_kw = ("monitor", "track", "worsening", "emerging", "watch", "ongoing", "alert")
    if any(k in tl for k in mon_kw):
        return "monitoring"

    if (
        "compare" in tl
        or "versus" in tl
        or " vs " in padded
        or " rank " in padded
        or re.search(r"\b(top|bottom|best|worst|highest|lowest)\b", tl)
    ):
        return "compare"

    loc_kw = (
        "where should",
        "which regions",
        "priority areas",
        "focus on",
        "where to",
        "which areas",
        "priority region",
    )
    if any(k in tl for k in loc_kw):
        return "locate"

    if (
        "should we" in tl
        or "what should" in tl
        or "recommend" in tl
        or "allocate" in tl
        or "prioritize" in tl
        or "what to prioritize" in tl
        or "policy design" in tl
        or re.search(r"\binvest(?:ing|ment)?\b", tl)
        or re.search(r"\bfund(?:ing)?\b", tl)
    ):
        return "decision_support"

    diag_kw = ("why", "because", "driver", "cause", "explain", "due to", "reason")
    if any(k in tl for k in diag_kw):
        return "diagnostic"

    desc_kw = (
        "trend",
        "changed",
        "over time",
        "how has",
        "what happened",
        "summary",
        "levels",
        "pattern",
        "show me",
        "describe",
    )
    if any(k in tl for k in desc_kw):
        return "descriptive"

    return "descriptive"


def _infer_domains(text: str) -> list[str]:
    tl = text.lower()
    out: list[str] = []
    for kw in _DOMAIN_KEYWORDS:
        if kw in tl and kw not in out:
            out.append(kw)
    return out[:8]


def _call_llama_decompose(query: str) -> dict[str, Any] | None:
    api_token = os.environ.get("HF_API_TOKEN")
    model_id = os.environ.get("RAG_LLM_MODEL_ID", "meta-llama/Llama-3.1-8B-Instruct")
    if not api_token:
        return None
    try:
        import requests
    except ImportError:
        return None
    intent_block = "\n".join(f"  - {line}" for line in _INTENT_LLM_LINES)
    allowed_csv = ", ".join(INTENT_ALLOWED)
    prompt = (
        "Users are government, NGOs, agribusiness, finance, and rural communities (often via programs). "
        "They rarely mention databases. Extract structured fields for retrieval and answering.\n\n"
        f"Return ONLY valid JSON with keys: intent (string, EXACTLY one of: {allowed_csv}), "
        "entities (array of strings), geography (array of country names or empty), "
        "domains (array of short topic tags), time_start (YYYY-MM-DD or empty string), "
        "time_end (YYYY-MM-DD or empty string).\n\n"
        "intent must be one of these values; meanings:\n"
        f"{intent_block}\n\n"
        "No markdown, no extra keys.\n\nQuestion: "
        + query
    )
    url = "https://router.huggingface.co/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
    payload = {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 400,
        "temperature": 0.0,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=45)
        resp.raise_for_status()
        data = resp.json()
        raw = str(data["choices"][0]["message"]["content"]).strip()
        if "```" in raw:
            m = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
            if m:
                raw = m.group(1).strip()
        return json.loads(raw)
    except Exception:
        return None


def decompose_query(query: str) -> dict[str, Any]:
    """
    Return facets: intent (one of INTENT_ALLOWED), entities, geography, domains,
    time_start, time_end (ISO dates or "").
    """
    q = (query or "").strip()
    if not q:
        return {
            "intent": "descriptive",
            "entities": [],
            "geography": [],
            "domains": [],
            "time_start": "",
            "time_end": "",
        }

    llm = _call_llama_decompose(q)
    countries = _extract_countries(q)
    domains = _infer_domains(q)
    ts, te = _extract_year_range(q)
    intent = _infer_intent(q)

    out: dict[str, Any] = {
        "intent": intent,
        "entities": [],
        "geography": countries,
        "domains": domains,
        "time_start": ts or "",
        "time_end": te or "",
    }

    if llm and isinstance(llm, dict):
        if isinstance(llm.get("intent"), str) and llm["intent"].strip():
            out["intent"] = llm["intent"].strip()
        if isinstance(llm.get("entities"), list):
            out["entities"] = [str(x) for x in llm["entities"][:20]]
        if isinstance(llm.get("geography"), list) and llm["geography"]:
            geo = [str(x).strip() for x in llm["geography"] if str(x).strip()]
            for c in geo:
                if c not in out["geography"]:
                    out["geography"].append(c)
        if isinstance(llm.get("domains"), list) and llm["domains"]:
            for d in llm["domains"][:10]:
                ds = str(d).strip().lower()
                if ds and ds not in out["domains"]:
                    out["domains"].append(ds)
        t0 = llm.get("time_start") or ""
        t1 = llm.get("time_end") or ""
        if isinstance(t0, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", t0.strip()):
            out["time_start"] = t0.strip()
        if isinstance(t1, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", t1.strip()):
            out["time_end"] = t1.strip()

    # Normalize empty time to heuristic if LLM omitted
    if not out["time_start"] and ts:
        out["time_start"] = ts
    if not out["time_end"] and te:
        out["time_end"] = te

    out["intent"] = _normalize_intent(out.get("intent"))
    return out

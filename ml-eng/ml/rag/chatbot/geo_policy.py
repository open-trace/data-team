"""Retrieval geography policy: profile country is a soft default, never a hard query replacement."""
from __future__ import annotations

import logging
from typing import Any

from ml.rag.chatbot.geo_iso3 import iso2_to_iso3, name_to_iso3
from ml.rag.chatbot.plan_policy import single_country_policy_enabled
from ml.rag.chatbot.query_decomposer import normalize_geography_for_filter

logger = logging.getLogger(__name__)

FARMER_PLAN_TYPE = "Farmers"


def sanitize_profile_country(raw: str | None) -> str | None:
    """Normalize profile/SSO country to African ISO3, or None.

    Prefer ``opentrace_ontology.clean_profile_country`` (NG / Nigeria / Nigéria → NGA;
    Albania / US / Unknown → None). Fallback still maps ISO2 via ``geo_iso3``.
    """
    if raw is None or not str(raw).strip():
        return None
    text = str(raw).strip()
    try:
        from opentrace_ontology import clean_profile_country

        cleaned = clean_profile_country(text)
        if cleaned:
            return cleaned
    except Exception:
        pass

    iso3 = name_to_iso3(text) or iso2_to_iso3(text)
    if iso3 is None:
        logger.info("profile_country_dropped_non_african", extra={"raw": text})
        return None
    return iso3


def profile_country_for_retrieval(
    plan_type: str | None,
    user_profile: dict[str, Any] | None,
) -> str:
    """
    Return a sanitized profile country for Farmers, or empty string.

    Only ``plan_type == Farmers`` may use ``user_profile.country``. Non-African
    values are dropped. This does not decide whether the value is a hard override.
    """
    if (plan_type or "").strip() != FARMER_PLAN_TYPE:
        return ""
    if not user_profile or not isinstance(user_profile, dict):
        return ""
    country = sanitize_profile_country(str(user_profile.get("country") or ""))
    if not country:
        return ""
    normalized = normalize_geography_for_filter([country])
    return normalized[0] if normalized else ""


def effective_geo_override(
    plan_type: str | None,
    user_profile: dict[str, Any] | None,
) -> str:
    """Hard geo override for graph state.

    Empty while ``ADZA_SINGLE_COUNTRY_POLICY`` is off (the default). Profile
    country is then only a soft default when the query names no country.
    Also empty when the profile country is not in the Africa gazetteer.
    """
    if not single_country_policy_enabled():
        return ""
    return profile_country_for_retrieval(plan_type, user_profile)


def apply_profile_geography_default(
    decomposition: dict[str, Any],
    profile_country: str | None,
) -> dict[str, Any]:
    """Fill empty geography from a sanitized profile country.

    Runs even when the single-country policy is off. Does not replace countries
    the query already named.
    """
    out = dict(decomposition or {})
    geo = out.get("geography")
    geo_list = [str(g) for g in geo] if isinstance(geo, list) else []
    if normalize_geography_for_filter(geo_list):
        out.setdefault("geography_source", "query")
        return out
    country = sanitize_profile_country(profile_country)
    if country:
        out["geography"] = [country]
        out["geography_source"] = "profile"
    return out


def clear_profile_ui_retrieval_scope(decomposition: dict[str, Any]) -> dict[str, Any]:
    """Drop profile/UI geo and time from retrieval fields. Keep them for gap labels."""
    out = dict(decomposition or {})
    geo_source = str(out.get("geography_source") or "")
    if geo_source in {"profile", "ui"}:
        geo = out.get("geography")
        if isinstance(geo, list):
            kept = [str(g).strip() for g in geo if str(g).strip()]
            if kept:
                out["filtered_geography"] = kept
        out["geography"] = []
    if str(out.get("time_source") or "") == "ui":
        ts = str(out.get("time_start") or "").strip()
        te = str(out.get("time_end") or "").strip()
        if ts:
            out["filtered_time_start"] = ts
        if te:
            out["filtered_time_end"] = te
        out["time_start"] = ""
        out["time_end"] = ""
    return out

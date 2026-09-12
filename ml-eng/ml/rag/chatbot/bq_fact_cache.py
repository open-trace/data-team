"""Session-scoped BQ fact-result cache (generalizes ranking follow-up reuse)."""
from __future__ import annotations

import hashlib
import json
from typing import Any

from ml.rag.chatbot.bq_ranking_cache import (
    bq_results_from_cache as ranking_results_from_cache,
    cache_entry_from_bq_results as ranking_cache_entry_from_bq_results,
    is_ranking_follow_up,
)
from ml.rag.session_store import get_json, session_ttl_seconds, set_json

_NS = "adza:bq:v1"


def _s(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _norm_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    for v in values:
        s = _s(v).lower()
        if s and s not in out:
            out.append(s)
    return sorted(out)


def bind_fingerprint(
    *,
    tables: list[str] | None = None,
    measure: str = "",
    geos: list[str] | None = None,
    time_start: str = "",
    time_end: str = "",
    shape: str = "",
    extra_enums: dict[str, Any] | None = None,
) -> str:
    payload = {
        "tables": _norm_list(tables),
        "measure": _s(measure).lower(),
        "geos": _norm_list(geos),
        "time_start": _s(time_start)[:10],
        "time_end": _s(time_end)[:10],
        "shape": _s(shape).lower(),
        "enums": {
            _s(k).lower(): _s(v).lower()
            for k, v in (extra_enums or {}).items()
            if _s(k)
        },
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:20]


def fingerprint_from_decomposition(
    decomposition: dict[str, Any] | None,
    *,
    tables: list[str] | None = None,
    measure: str = "",
    shape: str = "",
) -> str:
    dec: dict[str, Any] = {}
    if isinstance(decomposition, dict):
        dec = decomposition
    raw_geos = dec.get("geography")
    geo_list = [str(g) for g in raw_geos] if isinstance(raw_geos, list) else []
    measure_from_dec = ""
    raw_pm = dec.get("primary_measures")
    if isinstance(raw_pm, list) and len(raw_pm) > 0:
        measure_from_dec = _s(raw_pm[0])
    return bind_fingerprint(
        tables=tables,
        measure=measure or measure_from_dec,
        geos=geo_list,
        time_start=_s(dec.get("time_start")),
        time_end=_s(dec.get("time_end")),
        shape=shape or _s(dec.get("job")),
    )


def fact_cache_key(session_id: str | None, fingerprint: str) -> str | None:
    sid = _s(session_id)
    fp = _s(fingerprint)
    if not sid or not fp:
        return None
    return f"{_NS}:{sid}:{fp}"


def facets_subset_of_cached(
    current: dict[str, Any] | None,
    cached: dict[str, Any] | None,
) -> bool:
    """True when current facets refine or equal cached facets (same measure window)."""
    cur = current if isinstance(current, dict) else {}
    old = cached if isinstance(cached, dict) else {}
    if _s(cur.get("measure")).lower() and _s(old.get("measure")).lower():
        if _s(cur.get("measure")).lower() != _s(old.get("measure")).lower():
            return False
    cur_geos = set(_norm_list(cur.get("geos")))
    old_geos = set(_norm_list(old.get("geos")))
    if cur_geos and old_geos and not cur_geos.issubset(old_geos) and not old_geos.issubset(cur_geos):
        # Distinct country sets that don't nest → miss
        if cur_geos != old_geos:
            return False
    if cur_geos and old_geos and not cur_geos.issubset(old_geos) and len(cur_geos) > len(old_geos):
        return False
    if cur_geos and old_geos and not cur_geos.issubset(old_geos):
        return False
    for key in ("time_start", "time_end"):
        c = _s(cur.get(key))[:10]
        o = _s(old.get(key))[:10]
        if c and o and c != o:
            return False
    return True


def get_cached_facts(key: str | None) -> dict[str, Any] | None:
    if not key:
        return None
    raw = get_json(key)
    return raw if isinstance(raw, dict) else None


def set_cached_facts(key: str | None, entry: dict[str, Any]) -> None:
    if not key or not isinstance(entry, dict):
        return
    set_json(key, entry, ttl_s=session_ttl_seconds())


def fact_entry_from_bq_results(
    bq_results: list[dict[str, Any]],
    *,
    fingerprint: str,
    facets: dict[str, Any],
    query: str,
) -> dict[str, Any] | None:
    rows = [r for r in (bq_results or []) if isinstance(r, dict)]
    if not rows:
        return None
    return {
        "fingerprint": fingerprint,
        "facets": facets,
        "query": query,
        "items": rows[:30],
    }


def bq_results_from_fact_entry(entry: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(entry, dict):
        return []
    items = entry.get("items")
    if isinstance(items, list) and items:
        return [dict(x) for x in items if isinstance(x, dict)]
    # Ranking-shaped legacy entry
    return ranking_results_from_cache(entry)


def should_reuse_facts(
    *,
    context_applied: bool,
    current_facets: dict[str, Any],
    cached_entry: dict[str, Any] | None,
) -> bool:
    if not isinstance(cached_entry, dict):
        return False
    cached_facets = cached_entry.get("facets")
    if not isinstance(cached_facets, dict):
        cached_facets = {}
    if context_applied:
        return facets_subset_of_cached(current_facets, cached_facets) or facets_subset_of_cached(
            cached_facets, current_facets
        )
    return facets_subset_of_cached(current_facets, cached_facets)


__all__ = [
    "bind_fingerprint",
    "bq_results_from_fact_entry",
    "fact_cache_key",
    "fact_entry_from_bq_results",
    "facets_subset_of_cached",
    "fingerprint_from_decomposition",
    "get_cached_facts",
    "is_ranking_follow_up",
    "ranking_cache_entry_from_bq_results",
    "ranking_results_from_cache",
    "set_cached_facts",
    "should_reuse_facts",
]

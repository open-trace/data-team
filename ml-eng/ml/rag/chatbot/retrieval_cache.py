"""Session-scoped KV cache for vector retrieval hits."""
from __future__ import annotations

import hashlib
from typing import Any

from ml.rag.session_store import get_json, session_ttl_seconds, set_json

_NS = "adza:ret:v1"


def _s(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def text_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:16]


def retrieval_cache_key(
    *,
    session_id: str | None,
    text: str,
    corpora: str,
    geo: str = "",
    year: str = "",
) -> str | None:
    sid = _s(session_id)
    if not sid or not _s(text):
        return None
    return (
        f"{_NS}:{sid}:{text_hash(text)}:"
        f"{_s(corpora) or 'all'}:{_s(geo) or '-'}:{_s(year) or '-'}"
    )


def compact_hits(hits: list[dict[str, Any]], *, limit: int = 40) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    meta_keys = (
        "point_id",
        "doc_id",
        "doc_kind",
        "corpus",
        "geo_country",
        "title",
        "url",
        "published_at",
        "score",
        "constraint_relaxed",
    )
    for item in hits or []:
        if not isinstance(item, dict):
            continue
        meta: dict[str, Any] = {}
        raw_meta = item.get("metadata")
        if isinstance(raw_meta, dict):
            meta = raw_meta
        score = item.get("score")
        if score is None:
            score = meta.get("score")
        compact_meta: dict[str, Any] = {}
        for k in meta_keys:
            if k in meta and meta[k] is not None:
                compact_meta[k] = meta[k]
        out.append(
            {
                "content": str(item.get("content") or "")[:4000],
                "source": item.get("source"),
                "score": score,
                "metadata": compact_meta,
            }
        )
        if len(out) >= limit:
            break
    return out


def get_cached_hits(key: str | None) -> list[dict[str, Any]] | None:
    if not key:
        return None
    raw = get_json(key)
    if not isinstance(raw, dict):
        return None
    hits = raw.get("hits")
    if not isinstance(hits, list):
        return None
    return [h for h in hits if isinstance(h, dict)]


def set_cached_hits(key: str | None, hits: list[dict[str, Any]]) -> None:
    if not key:
        return
    set_json(key, {"hits": compact_hits(hits)}, ttl_s=session_ttl_seconds())


def geo_year_from_kwargs(kwargs: dict[str, Any]) -> tuple[str, str]:
    geos: list[str] = []
    if kwargs.get("geo_country"):
        geos.append(_s(kwargs.get("geo_country")))
    for g in kwargs.get("geo_countries") or []:
        s = _s(g)
        if s:
            geos.append(s)
    geo = ",".join(sorted(dict.fromkeys(geos)))
    year = _s(kwargs.get("published_at_from"))[:4]
    return geo, year


__all__ = [
    "compact_hits",
    "geo_year_from_kwargs",
    "get_cached_hits",
    "retrieval_cache_key",
    "set_cached_hits",
    "text_hash",
]

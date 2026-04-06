from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from difflib import SequenceMatcher
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


_TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
}


def normalize_url_for_dedupe(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    parts = urlparse(u)
    host = parts.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    qs = [(k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True) if k.lower() not in _TRACKING_PARAMS]
    query = urlencode(qs)
    path = re.sub(r"/+", "/", parts.path or "/").rstrip("/") or "/"
    return urlunparse((parts.scheme.lower() or "https", host, path, "", query, ""))


def _norm_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def title_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, _norm_text(a), _norm_text(b)).ratio()


def content_hash(text: str) -> str:
    t = re.sub(r"\s+", " ", (text or "").strip().lower())
    return hashlib.sha256(t.encode("utf-8")).hexdigest()


def dedupe_id(title: str, source_host: str) -> str:
    payload = f"{_norm_text(title)}|{(source_host or '').lower()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def cluster_id(title: str, country: str, domain: str, published_at_iso: str | None) -> str:
    day_bucket = "unknown"
    if published_at_iso:
        try:
            dt = datetime.fromisoformat(published_at_iso.replace("Z", "+00:00")).astimezone(UTC)
            day_bucket = dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    payload = f"{_norm_text(title)[:120]}|{country.lower()}|{domain.lower()}|{day_bucket}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

"""
Heuristics for Tavily *search* snippet bodies (not full HTML).

Search results often mix unrelated URLs; we reject obvious mashups unless opted in with checks.
"""
from __future__ import annotations

import re
from typing import Tuple

_URL_HOST = re.compile(r"https?://([^/\s?#]+)", re.IGNORECASE)
# Words from title that must match for "relevance" (skip very short tokens)
_MIN_TOKEN_LEN = 4
_MAX_TITLE_TOKENS = 8


def _distinct_hosts(text: str) -> set[str]:
    hosts: set[str] = set()
    for m in _URL_HOST.finditer(text or ""):
        h = m.group(1).lower().split("@")[-1].lstrip("www.")
        if h:
            hosts.add(h)
    return hosts


def _country_in_body(body_lower: str, country: str) -> bool:
    c = (country or "").strip().lower()
    if len(c) >= 3 and c in body_lower:
        return True
    for part in re.split(r"[\s,]+", country or ""):
        p = part.strip().lower()
        if len(p) >= 4 and p in body_lower:
            return True
    return False


def _title_tokens(title: str) -> list[str]:
    raw = re.findall(r"[A-Za-z][A-Za-z\-']+", (title or ""))
    out: list[str] = []
    for w in raw:
        wl = w.lower()
        if len(wl) >= _MIN_TOKEN_LEN and wl not in {
            "news",
            "google",
            "report",
            "says",
            "from",
            "with",
            "that",
            "this",
            "after",
            "before",
            "into",
        }:
            out.append(wl)
        if len(out) >= _MAX_TITLE_TOKENS:
            break
    return out


def tavily_search_body_passes_quality(
    *,
    body: str,
    country: str,
    title: str,
    max_distinct_domains: int,
    require_country_or_title_match: bool,
) -> Tuple[bool, str]:
    """
    Returns (ok, reason_if_not_ok).
    """
    b = (body or "").strip()
    if not b:
        return False, "empty body"

    hosts = _distinct_hosts(b)
    n = len(hosts)
    if n > max_distinct_domains:
        return (
            False,
            f"too many distinct URL domains in Tavily search text ({n} > {max_distinct_domains})",
        )

    if not require_country_or_title_match:
        return True, ""

    bl = b.lower()
    if _country_in_body(bl, country):
        return True, ""

    tokens = _title_tokens(title)
    if not tokens:
        return False, "no title tokens to verify relevance"

    hits = sum(1 for t in tokens if t in bl)
    min_hits = 2 if len(tokens) >= 2 else 1
    if hits >= min_hits:
        return True, ""

    return (
        False,
        f"Tavily search text may be off-topic (country/title terms missing; title token hits={hits})",
    )

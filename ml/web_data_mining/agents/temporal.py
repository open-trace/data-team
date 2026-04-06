from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _to_utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.replace(microsecond=0).isoformat()


def _parse_date_str(raw: str) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    for candidate in (s, s[:19], s[:10]):
        try:
            if len(candidate) == 10:
                d = datetime.strptime(candidate, "%Y-%m-%d")
                return d.replace(tzinfo=UTC)
            return datetime.fromisoformat(candidate)
        except Exception:
            continue
    return None


_META_DATE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']',
        re.I,
    ),
    re.compile(
        r'<meta[^>]+property=["\']og:published_time["\'][^>]+content=["\']([^"\']+)["\']',
        re.I,
    ),
    re.compile(
        r'<meta[^>]+name=["\']pubdate["\'][^>]+content=["\']([^"\']+)["\']',
        re.I,
    ),
    re.compile(
        r'<meta[^>]+name=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']',
        re.I,
    ),
)

_META_UPDATED_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r'<meta[^>]+property=["\']article:modified_time["\'][^>]+content=["\']([^"\']+)["\']',
        re.I,
    ),
    re.compile(
        r'<meta[^>]+property=["\']og:updated_time["\'][^>]+content=["\']([^"\']+)["\']',
        re.I,
    ),
)


def extract_html_dates(html: str) -> tuple[str | None, str | None, dict[str, str | None]]:
    """Return (published_iso, updated_iso, raw_fields)."""
    raw_pub: str | None = None
    raw_upd: str | None = None

    for pat in _META_DATE_PATTERNS:
        m = pat.search(html or "")
        if m:
            raw_pub = m.group(1).strip()
            break
    for pat in _META_UPDATED_PATTERNS:
        m = pat.search(html or "")
        if m:
            raw_upd = m.group(1).strip()
            break

    pub_dt = _parse_date_str(raw_pub or "")
    upd_dt = _parse_date_str(raw_upd or "")
    pub_iso = _to_utc_iso(pub_dt) if pub_dt else None
    upd_iso = _to_utc_iso(upd_dt) if upd_dt else None
    return pub_iso, upd_iso, {"html_published_raw": raw_pub, "html_updated_raw": raw_upd}


@dataclass
class PublishedAtDecision:
    published_at: str | None
    published_at_source: str
    published_at_confidence: float
    published_at_raw: str | None


def pick_published_at(
    *,
    rss_date: date | None,
    html_published_iso: str | None,
    tavily_published_iso: str | None = None,
    inferred_iso: str | None = None,
    now_utc: datetime | None = None,
) -> PublishedAtDecision:
    """
    Source priority: rss > html > tavily > inferred > missing.
    Reject candidates too far in future.
    """
    now = now_utc or datetime.now(UTC)
    max_future = now + timedelta(days=2)

    def _valid(iso: str | None) -> bool:
        if not iso:
            return False
        dt = _parse_date_str(iso)
        if not dt:
            return False
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        else:
            dt = dt.astimezone(UTC)
        return dt <= max_future

    if rss_date is not None:
        dt = datetime(rss_date.year, rss_date.month, rss_date.day, tzinfo=UTC)
        iso = _to_utc_iso(dt)
        if _valid(iso):
            return PublishedAtDecision(iso, "rss", 0.95, rss_date.isoformat())
    if _valid(html_published_iso):
        return PublishedAtDecision(html_published_iso, "html", 0.85, html_published_iso)
    if _valid(tavily_published_iso):
        return PublishedAtDecision(tavily_published_iso, "tavily", 0.75, tavily_published_iso)
    if _valid(inferred_iso):
        return PublishedAtDecision(inferred_iso, "inferred", 0.5, inferred_iso)
    return PublishedAtDecision(None, "missing", 0.0, None)

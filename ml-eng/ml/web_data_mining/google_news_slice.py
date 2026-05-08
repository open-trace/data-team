"""
Build per-day Google News RSS search URLs using after:/before: operators.

Standard outlet RSS feeds only return a *recent* window of items; they do not expose
a full archive back to 2000. Day-slicing can help Google News search RSS return
smaller, date-bounded result sets. Behavior is best-effort and subject to Google's
limits, blocking, and query semantics — chunk large ranges into months/years.
"""
from __future__ import annotations

from datetime import date, timedelta
from urllib.parse import parse_qsl, urlencode, urlparse


def is_google_news_search_rss(url: str) -> bool:
    u = (url or "").lower()
    return "news.google.com" in u and "/rss/search" in u and "q=" in u


def _merge_q(base_q: str, day: date) -> str:
    """Append after:/before: for [day, day+1) in Google's query syntax."""
    nxt = day + timedelta(days=1)
    extra = f"after:{day.isoformat()} before:{nxt.isoformat()}"
    b = (base_q or "").strip()
    if not b:
        return extra
    # Avoid duplicating operators if URL was already hand-edited
    low = b.lower()
    if "after:" in low and "before:" in low:
        return b
    return f"{b} {extra}"


def expand_google_news_rss_urls(base_url: str, start: date, end: date) -> list[tuple[str, str]]:
    """
    Return [(fetch_url, feed_label_suffix), ...] for each calendar day in [start, end].

    feed_label_suffix is e.g. "[2000-01-01]" for logging / metadata.
    """
    if start > end:
        return []
    out: list[tuple[str, str]] = []
    parts = urlparse(base_url.strip())
    q_pairs = parse_qsl(parts.query, keep_blank_values=True)
    qdict: dict[str, str] = dict(q_pairs)
    base_q = qdict.get("q", "")

    d = start
    while d <= end:
        qdict["q"] = _merge_q(base_q, d)
        new_query = urlencode(list(qdict.items()))
        # Keep all URL components unchanged except query; avoids type-checker overload ambiguity.
        new_url = parts._replace(query=new_query).geturl()
        out.append((new_url, f"[{d.isoformat()}]"))
        d += timedelta(days=1)
    return out


def slice_range_from_params(
    start_date: date | None,
    end_date: date | None,
    start_year: int | None,
    end_year: int | None,
) -> tuple[date, date]:
    """Resolve inclusive calendar range for slicing from RunParams-style fields."""
    if start_date is not None and end_date is not None:
        return start_date, end_date
    if start_year is not None and end_year is not None:
        return date(start_year, 1, 1), date(end_year, 12, 31)
    raise ValueError("Need (start_date, end_date) or (start_year, end_year) for Google News slicing.")


def count_slice_days(start: date, end: date) -> int:
    return (end - start).days + 1 if end >= start else 0

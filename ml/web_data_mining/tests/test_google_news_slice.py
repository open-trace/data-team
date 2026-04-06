"""Google News RSS day-slicing URL builder."""
from __future__ import annotations

from datetime import date

from ml.web_data_mining.google_news_slice import (
    expand_google_news_rss_urls,
    is_google_news_search_rss,
    slice_range_from_params,
)


def test_is_google_news_search_rss() -> None:
    assert is_google_news_search_rss(
        "https://news.google.com/rss/search?q=Foo&hl=en&gl=NG&ceid=NG:en"
    )
    assert not is_google_news_search_rss("https://www.vanguardngr.com/feed/")


def test_expand_single_day_appends_after_before() -> None:
    base = "https://news.google.com/rss/search?q=Nigeria+agriculture&hl=en&gl=NG&ceid=NG:en"
    out = expand_google_news_rss_urls(base, date(2024, 6, 1), date(2024, 6, 1))
    assert len(out) == 1
    url, label = out[0]
    assert label == "[2024-06-01]"
    assert "after%3A2024-06-01" in url or "after:2024-06-01" in url
    assert "before%3A2024-06-02" in url or "before:2024-06-02" in url


def test_slice_range_from_years() -> None:
    a, b = slice_range_from_params(None, None, 2000, 2001)
    assert a == date(2000, 1, 1)
    assert b == date(2001, 12, 31)

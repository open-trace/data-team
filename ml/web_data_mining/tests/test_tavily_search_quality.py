"""Tests for Tavily search snippet quality heuristics."""
from __future__ import annotations

from ml.web_data_mining.agentic.tavily_search_quality import tavily_search_body_passes_quality


def test_rejects_many_domains() -> None:
    body = (
        "One https://a.com/x two https://b.com/y three https://c.com/z "
        "Nigeria agriculture story"
    )
    ok, reason = tavily_search_body_passes_quality(
        body=body,
        country="Nigeria",
        title="Farm news",
        max_distinct_domains=2,
        require_country_or_title_match=True,
    )
    assert ok is False
    assert "domain" in reason.lower()


def test_accepts_nigeria_in_text() -> None:
    body = "Summary about Nigeria and crops. https://tribune.com.pk/story/1"
    ok, _ = tavily_search_body_passes_quality(
        body=body,
        country="Nigeria",
        title="Prospects of agriculture",
        max_distinct_domains=2,
        require_country_or_title_match=True,
    )
    assert ok is True


def test_requires_relevance_can_false() -> None:
    body = "https://a.com/x https://b.com/y unrelated"
    ok, _ = tavily_search_body_passes_quality(
        body=body,
        country="Nigeria",
        title="Prospects of agriculture",
        max_distinct_domains=5,
        require_country_or_title_match=False,
    )
    assert ok is True

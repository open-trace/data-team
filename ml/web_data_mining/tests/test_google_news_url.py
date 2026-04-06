"""RSS URL resolution for Google News gate links."""
from __future__ import annotations

from ml.web_data_mining.agents.rss_discovery import RssItem, article_url_from_feed_entry, resolve_rss_item_fetch_url


def test_keeps_normal_publisher_link() -> None:
    u = "https://www.vanguardngr.com/some-article/"
    assert article_url_from_feed_entry({"link": u}, u) == u


def test_prefers_source_href_over_google_link() -> None:
    google = "https://news.google.com/rss/articles/CBMiFA"
    pub = "https://punchng.com/agriculture/foo/"
    entry = {"link": google, "source": {"href": pub}}
    assert article_url_from_feed_entry(entry, google) == pub


def test_first_non_google_link_in_links() -> None:
    google = "https://news.google.com/rss/articles/XYZ"
    pub = "https://businessday.ng/news/1/"
    entry = {
        "link": google,
        "links": [
            {"href": google, "rel": "alternate"},
            {"href": pub, "type": "text/html"},
        ],
    }
    assert article_url_from_feed_entry(entry, google) == pub


def test_href_in_summary_when_source_is_site_root() -> None:
    google = "https://news.google.com/rss/articles/X"
    article = "https://guardian.ng/environment/solar-pumps-story/"
    entry = {
        "link": google,
        "source": {"href": "https://guardian.ng"},
        "summary": f'<p><a href="{article}">Full story</a></p>',
    }
    assert article_url_from_feed_entry(entry, google) == article


def test_resolve_item_uses_plaintext_dailytrust_url() -> None:
    google = "https://news.google.com/rss/articles/ZZZ"
    dt = "https://dailytrust.com/why-farmers-have-abandoned-poultry-fish-for-rabbit-production-in-kano/"
    item = RssItem(
        url=google,
        title="Rabbit farming in Kano",
        summary=f"Full story {dt}",
        published=None,
        feed_name="Google News",
        extra_fetch_url_hints=(),
    )
    assert resolve_rss_item_fetch_url(item) == dt

from __future__ import annotations

import re
from html.parser import HTMLParser


class _TextCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._chunks: list[str] = []
        self._skip_depth = 0
        self._skip_tags = frozenset({"script", "style", "nav", "footer", "header", "aside", "noscript"})

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t in self._skip_tags:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in self._skip_tags and self._skip_depth > 0:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0 and data.strip():
            self._chunks.append(data)

    def text(self) -> str:
        raw = " ".join(self._chunks)
        return re.sub(r"\s+", " ", raw).strip()


def html_to_plain_text(html: str) -> str:
    parser = _TextCollector()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        return ""
    return parser.text()


def extract_title_regex(html: str) -> str:
    m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip()


def extract_og_title_regex(html: str) -> str:
    m = re.search(
        r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        html,
        re.IGNORECASE,
    )
    if not m:
        m = re.search(
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']',
            html,
            re.IGNORECASE,
        )
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip()

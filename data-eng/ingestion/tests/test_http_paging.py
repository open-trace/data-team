"""HTTP retry and paging tests."""

from __future__ import annotations

import responses

from ingestion.common.http import HttpError, get_with_backoff
from ingestion.common.paging import iter_offset_limit, iter_page_num, iter_skip_top


@responses.activate
def test_http_retries_then_succeeds() -> None:
    url = "https://example.test/data"
    responses.add(responses.GET, url, status=429)
    responses.add(responses.GET, url, status=503)
    responses.add(responses.GET, url, json={"ok": True}, status=200)
    sleeps: list[float] = []
    resp = get_with_backoff(url, max_retries=3, sleep_fn=sleeps.append)
    assert resp.status_code == 200
    assert len(sleeps) == 2


@responses.activate
def test_http_raises_after_max_retries() -> None:
    url = "https://example.test/fail"
    for _ in range(4):
        responses.add(responses.GET, url, status=503)
    with __import__("pytest").raises(HttpError, match="after"):
        get_with_backoff(url, max_retries=2, sleep_fn=lambda _: None)


@responses.activate
def test_http_terminal_4xx() -> None:
    url = "https://example.test/nope"
    responses.add(responses.GET, url, status=404, body="missing")
    with __import__("pytest").raises(HttpError, match="terminal"):
        get_with_backoff(url, sleep_fn=lambda _: None)


def test_page_num_stops_on_short_page() -> None:
    pages = {
        1: [{"id": 1}, {"id": 2}],
        2: [{"id": 3}],
    }
    rows = list(iter_page_num(lambda p: pages.get(p, []), per_page=2))
    assert [r["id"] for r in rows] == [1, 2, 3]


def test_page_num_honours_limit() -> None:
    pages = {1: [{"id": i} for i in range(5)]}
    rows = list(iter_page_num(lambda p: pages[p], per_page=5, limit=3))
    assert len(rows) == 3


def test_offset_limit_stops_when_offset_ge_found() -> None:
    def fetch(offset: int, limit: int):
        data = [{"i": i} for i in range(offset, min(offset + limit, 5))]
        return data, 5

    rows = list(iter_offset_limit(fetch, page_size=2))
    assert len(rows) == 5


def test_offset_limit_honours_limit() -> None:
    def fetch(offset: int, limit: int):
        data = [{"i": i} for i in range(offset, offset + limit)]
        return data, 1000

    rows = list(iter_offset_limit(fetch, page_size=100, limit=10))
    assert len(rows) == 10


def test_skip_top_pages_until_count() -> None:
    def fetch(skip: int, top: int):
        end = min(skip + top, 5)
        data = [{"i": i} for i in range(skip, end)]
        return data, 5

    rows = list(iter_skip_top(fetch, page_size=2))
    assert [r["i"] for r in rows] == [0, 1, 2, 3, 4]


def test_skip_top_honours_limit() -> None:
    def fetch(skip: int, top: int):
        data = [{"i": i} for i in range(skip, skip + top)]
        return data, 1000

    rows = list(iter_skip_top(fetch, page_size=100, limit=10))
    assert len(rows) == 10

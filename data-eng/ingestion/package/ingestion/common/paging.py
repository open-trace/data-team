"""Pagination helpers for page_num and offset/limit APIs."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, TypeVar

T = TypeVar("T")


def iter_page_num(
    fetch_page: Callable[[int], list[T]],
    *,
    per_page: int,
    start_page: int = 1,
    limit: int | None = None,
) -> Iterator[T]:
    """Yield items until a short page (< per_page) or limit reached."""
    page = start_page
    yielded = 0
    while True:
        rows = fetch_page(page)
        if not rows:
            return
        for row in rows:
            yield row
            yielded += 1
            if limit is not None and yielded >= limit:
                return
        if len(rows) < per_page:
            return
        page += 1


def iter_offset_limit(
    fetch_page: Callable[[int, int], tuple[list[T], int]],
    *,
    page_size: int = 1000,
    limit: int | None = None,
) -> Iterator[T]:
    """Yield items using offset/limit; stop when offset >= found or short page.

    fetch_page(offset, limit) -> (rows, found_total)
    """
    offset = 0
    yielded = 0
    while True:
        remaining = None if limit is None else max(limit - yielded, 0)
        if remaining == 0:
            return
        batch = page_size if remaining is None else min(page_size, remaining)
        rows, found = fetch_page(offset, batch)
        if not rows:
            return
        for row in rows:
            yield row
            yielded += 1
            if limit is not None and yielded >= limit:
                return
        offset += len(rows)
        if offset >= found:
            return
        if len(rows) < batch:
            return


def iter_skip_top(
    fetch_page: Callable[[int, int], tuple[list[T], int]],
    *,
    page_size: int = 1000,
    limit: int | None = None,
) -> Iterator[T]:
    """Yield items using Data360-style skip/top; stop when skip >= count or short page.

    fetch_page(skip, top) -> (rows, total_count)
    """
    skip = 0
    yielded = 0
    while True:
        remaining = None if limit is None else max(limit - yielded, 0)
        if remaining == 0:
            return
        top = page_size if remaining is None else min(page_size, remaining)
        rows, total = fetch_page(skip, top)
        if not rows:
            return
        for row in rows:
            yield row
            yielded += 1
            if limit is not None and yielded >= limit:
                return
        skip += len(rows)
        if total > 0 and skip >= total:
            return
        if len(rows) < top:
            return


def parse_found(payload: dict[str, Any], default: int = 0) -> int:
    for key in ("found", "total", "total_count", "count"):
        if key in payload and payload[key] is not None:
            return int(payload[key])
    return default

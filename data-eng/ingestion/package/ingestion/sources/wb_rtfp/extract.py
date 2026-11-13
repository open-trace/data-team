"""Extract World Bank RTFP JSON API with offset/limit paging."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

import pandas as pd

from ingestion.common.http import get_with_backoff, make_session
from ingestion.common.paging import iter_offset_limit, parse_found

PAGE_SIZE = 1000


def extract(ctx: Any) -> Iterable[pd.DataFrame]:
    if ctx.settings.offline_fixtures:
        root = ctx.settings.package_root / "tests" / "fixtures"
        rows: list[dict] = []
        for name in ("rtfp_page1.json", "rtfp_page2.json"):
            payload = json.loads((root / name).read_text(encoding="utf-8"))
            rows.extend(payload.get("data") or payload.get("records") or [])
        df = pd.DataFrame(rows)
        if ctx.limit is not None:
            df = df.iloc[: ctx.limit]
        yield df
        return

    session = make_session()
    url = ctx.source.url

    def fetch_page(offset: int, limit: int) -> tuple[list[dict], int]:
        resp = get_with_backoff(
            url,
            session=session,
            params={"offset": offset, "limit": limit},
        )
        payload = resp.json()
        data = payload.get("data") or payload.get("records") or []
        found = parse_found(payload, default=offset + len(data))
        return list(data), found

    rows = list(iter_offset_limit(fetch_page, page_size=PAGE_SIZE, limit=ctx.limit))
    yield pd.DataFrame(rows)

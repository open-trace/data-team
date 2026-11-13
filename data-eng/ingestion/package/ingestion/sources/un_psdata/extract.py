"""Extract UN Peace & Security paged JSON (notebook port)."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

import pandas as pd

from ingestion.common.http import get_with_backoff, make_session
from ingestion.common.paging import iter_page_num

PER_PAGE = 250


def _dataset_ids(ctx: Any, session) -> list[str]:
    meta_url = ctx.source.config.get("metadata_url") or f"{ctx.source.url}/metadata/all"
    if ctx.settings.offline_fixtures:
        return ["DS1", "DS2"]
    resp = get_with_backoff(meta_url, session=session)
    payload = resp.json()
    # Notebook collected Dataset IDs from metadata list
    if isinstance(payload, list):
        items = payload
    else:
        items = payload.get("datasets") or payload.get("data") or []
    ids: list[str] = []
    for item in items:
        if isinstance(item, dict):
            did = item.get("DatasetId") or item.get("dataset_id") or item.get("id")
            if did is not None:
                ids.append(str(did))
        else:
            ids.append(str(item))
    return ids


def extract(ctx: Any) -> Iterable[pd.DataFrame]:
    if ctx.settings.offline_fixtures:
        root = ctx.settings.package_root / "tests" / "fixtures"
        rows: list[dict] = []
        for name in ("un_page1.json", "un_page2.json"):
            payload = json.loads((root / name).read_text(encoding="utf-8"))
            rows.extend(payload.get("data") or payload.get("records") or [])
        df = pd.DataFrame(rows)
        if ctx.limit is not None:
            df = df.iloc[: ctx.limit]
        yield df
        return

    session = make_session()
    base = ctx.source.url.rstrip("/")
    all_rows: list[dict] = []
    for dataset_id in _dataset_ids(ctx, session):

        def fetch_page(page: int, _ds: str = dataset_id) -> list[dict]:
            url = f"{base}/data/{_ds}/json"
            resp = get_with_backoff(
                url,
                session=session,
                params={"per_page": PER_PAGE, "page_num": page},
            )
            payload = resp.json()
            data = payload.get("data") or payload.get("records") or payload
            if not isinstance(data, list):
                return []
            out = []
            for rec in data:
                out.append({"dataset_id": _ds, "record": rec})
            return out

        for item in iter_page_num(fetch_page, per_page=PER_PAGE, limit=ctx.limit):
            all_rows.append(item)
            if ctx.limit is not None and len(all_rows) >= ctx.limit:
                yield pd.DataFrame(all_rows)
                return

    yield pd.DataFrame(all_rows)

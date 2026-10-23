"""Extract World Bank Data360 Harmonized Country Poverty (full WB_HCP dump)."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

import pandas as pd

from ingestion.common.http import get_with_backoff, make_session
from ingestion.common.paging import iter_skip_top, parse_found

FIXTURE_NAME = "world_bank_data360_harmonized_country_poverty_sample.json"
PAGE_SIZE = 1000


def _cfg(ctx: Any) -> dict[str, Any]:
    return dict(getattr(ctx.source, "config", None) or {})


def extract(ctx: Any) -> Iterable[pd.DataFrame]:
    cfg = _cfg(ctx)
    if ctx.settings.offline_fixtures:
        path = ctx.settings.package_root / "tests" / "fixtures" / FIXTURE_NAME
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload.get("value") or payload.get("data") or []
        df = pd.DataFrame(rows)
        if ctx.limit is not None:
            df = df.iloc[: ctx.limit]
        yield df
        return

    url = ctx.source.url or "https://data360api.worldbank.org/data360/data"
    database_id = str(cfg.get("database_id") or "WB_HCP")
    session = make_session()
    base_params: dict[str, Any] = {"DATABASE_ID": database_id}
    # Optional filters only when explicitly set in source.yaml
    if cfg.get("indicator_id"):
        base_params["INDICATOR"] = str(cfg["indicator_id"])
    if cfg.get("ref_areas"):
        base_params["REF_AREA"] = ",".join(str(a) for a in cfg["ref_areas"])

    def fetch_page(skip: int, top: int) -> tuple[list[dict[str, Any]], int]:
        params = {**base_params, "skip": skip, "top": top}
        resp = get_with_backoff(url, session=session, params=params)
        payload = resp.json()
        rows = list(payload.get("value") or payload.get("data") or [])
        return rows, parse_found(payload)

    rows = list(
        iter_skip_top(fetch_page, page_size=PAGE_SIZE, limit=ctx.limit)
    )
    yield pd.DataFrame(rows)

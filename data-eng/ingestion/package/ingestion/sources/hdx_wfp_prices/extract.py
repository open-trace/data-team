"""Extract HDX WFP Nigeria food prices CSV."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ingestion.common.http import get_with_backoff

FIXTURE_NAME = "hdx_wfp_nga_sample.csv"


def extract(ctx: Any) -> Iterable[bytes]:
    if ctx.settings.offline_fixtures:
        path = ctx.settings.package_root / "tests" / "fixtures" / FIXTURE_NAME
        yield path.read_bytes()
        return
    url = ctx.source.url
    resp = get_with_backoff(url)
    yield resp.content

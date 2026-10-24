"""Extract World Bank Pink Sheet XLSX."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ingestion.common.http import get_with_backoff

FIXTURE_NAME = "pink_sheet_sample.xlsx"


def extract(ctx: Any) -> Iterable[bytes]:
    if ctx.settings.offline_fixtures:
        path = ctx.settings.package_root / "tests" / "fixtures" / FIXTURE_NAME
        yield path.read_bytes()
        return
    resp = get_with_backoff(ctx.source.url)
    yield resp.content

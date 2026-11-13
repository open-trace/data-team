"""Copy-me extract skeleton."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd

from ingestion.common.http import get_with_backoff


def extract(ctx: Any) -> Iterable[pd.DataFrame | bytes]:
    url = ctx.source.url
    if ctx.settings.offline_fixtures:
        fixture = ctx.settings.package_root / "tests" / "fixtures" / "template.csv"
        yield fixture.read_bytes()
        return
    resp = get_with_backoff(url)
    yield resp.content

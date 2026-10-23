"""Copy-me transform skeleton."""

from __future__ import annotations

from io import BytesIO
from typing import Any

import pandas as pd


def transform(ctx: Any, raw: pd.DataFrame | bytes) -> pd.DataFrame:
    if isinstance(raw, pd.DataFrame):
        return raw
    return pd.read_csv(BytesIO(raw))

"""Transform HDX WFP food prices CSV (skip HXL tag row)."""

from __future__ import annotations

from io import StringIO
from typing import Any

import pandas as pd

TYPED = {
    "date": "datetime64[ns]",
    "market_id": "Int64",
    "commodity_id": "Int64",
    "price": "float64",
    "usdprice": "float64",
    "latitude": "float64",
    "longitude": "float64",
}


def _skip_hxl(text: str) -> str:
    lines = text.splitlines()
    if len(lines) >= 2 and lines[1].lstrip().startswith("#"):
        return "\n".join([lines[0], *lines[2:]])
    return text


def transform(ctx: Any, raw: bytes | pd.DataFrame) -> pd.DataFrame:
    if isinstance(raw, pd.DataFrame):
        df = raw.copy()
    else:
        text = raw.decode("utf-8", errors="replace")
        text = _skip_hxl(text)
        df = pd.read_csv(StringIO(text))

    # Normalize column names
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col, dtype in TYPED.items():
        if col in df.columns and col != "date":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

    for col in ("market", "commodity", "pricetype", "unit", "currency"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df

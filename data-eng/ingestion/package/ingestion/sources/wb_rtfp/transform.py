"""Normalize RTFP API rows."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

# Common field aliases seen in RTFP-style payloads
ALIASES = {
    "country": ["country", "country_name", "adm0_name", "ISO3"],
    "market": ["market", "market_name", "mkt_name"],
    "date": ["date", "price_date", "obs_date"],
    "commodity": ["commodity", "commodity_name", "cm_name", "product"],
}


def _pick(df: pd.DataFrame, names: list[str]) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def transform(ctx: Any, raw: pd.DataFrame | bytes) -> pd.DataFrame:
    if isinstance(raw, (bytes, bytearray)):
        payload = json.loads(raw.decode("utf-8"))
        raw = pd.DataFrame(payload.get("data") or payload.get("records") or [])

    df = raw.copy()
    if df.empty:
        return df

    rename: dict[str, str] = {}
    for canon, aliases in ALIASES.items():
        found = _pick(df, aliases)
        if found and found != canon:
            rename[found] = canon
    df = df.rename(columns=rename)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    return df

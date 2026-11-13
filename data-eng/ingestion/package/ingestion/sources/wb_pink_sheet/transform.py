"""Wide→long melt of Pink Sheet Monthly Prices."""

from __future__ import annotations

from io import BytesIO
from typing import Any

import pandas as pd


def _period_to_date(period: str) -> pd.Timestamp | pd.NaT:
    # Formats like 2024M12 or 2024M01
    s = str(period).strip()
    try:
        if "M" in s:
            year, month = s.split("M", 1)
            return pd.Timestamp(year=int(year), month=int(month), day=1)
    except Exception:
        return pd.NaT
    return pd.NaT


def transform(ctx: Any, raw: bytes | pd.DataFrame) -> pd.DataFrame:
    if isinstance(raw, pd.DataFrame):
        # Already long-form for tests
        if {"period", "series", "value"}.issubset(set(raw.columns)):
            out = raw.copy()
            if "period_date" not in out.columns:
                out["period_date"] = out["period"].map(_period_to_date)
            return out

    bio = BytesIO(raw) if isinstance(raw, (bytes, bytearray)) else None
    # Read without header; detect series + units rows
    wide = pd.read_excel(bio or raw, sheet_name="Monthly Prices", header=None)
    # Find first row that looks like a period (contains 'M' between digits)
    header_row = None
    for i in range(min(10, len(wide))):
        cell = str(wide.iloc[i, 0])
        if "M" in cell and any(ch.isdigit() for ch in cell):
            header_row = i
            break
    if header_row is None:
        # Assume row 0 = series names, row 1 = units, data from row 2
        series_row, units_row, data_start = 0, 1, 2
    else:
        series_row = max(header_row - 2, 0)
        units_row = max(header_row - 1, 0)
        data_start = header_row

    series = [str(x).strip() for x in wide.iloc[series_row, 1:].tolist()]
    units = [str(x).strip() for x in wide.iloc[units_row, 1:].tolist()]
    data = wide.iloc[data_start:].copy()
    data.columns = ["period", *series]
    long = data.melt(id_vars=["period"], var_name="series", value_name="value")
    unit_map = dict(zip(series, units, strict=False))
    long["unit"] = long["series"].map(unit_map)
    long["period"] = long["period"].astype(str).str.strip()
    long = long[long["period"].str.contains("M", na=False)]
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long["period_date"] = long["period"].map(_period_to_date)
    long = long.dropna(subset=["value"])
    return long.reset_index(drop=True)[["period", "period_date", "series", "unit", "value"]]

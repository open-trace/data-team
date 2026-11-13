"""Normalize Data360 observation rows into a flat landing table."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

COL_MAP = {
    "DATABASE_ID": "database_id",
    "INDICATOR": "indicator",
    "REF_AREA": "ref_area",
    "SEX": "sex",
    "AGE": "age",
    "URBANISATION": "urbanisation",
    "TIME_PERIOD": "time_period",
    "FREQ": "freq",
    "OBS_VALUE": "obs_value",
    "UNIT_MEASURE": "unit_measure",
    "OBS_STATUS": "obs_status",
    "DATA_SOURCE": "data_source",
}


def transform(ctx: Any, raw: bytes | pd.DataFrame | dict) -> pd.DataFrame:
    if isinstance(raw, pd.DataFrame):
        df = raw.copy()
    elif isinstance(raw, dict):
        rows = raw.get("value") or raw.get("data") or []
        df = pd.DataFrame(rows)
    elif isinstance(raw, (bytes, bytearray)):
        payload = json.loads(raw.decode("utf-8"))
        rows = payload.get("value") or payload.get("data") or []
        df = pd.DataFrame(rows)
    else:
        raise TypeError(f"Unsupported raw type: {type(raw)}")

    if df.empty:
        return df

    rename = {c: COL_MAP[c] for c in df.columns if c in COL_MAP}
    out = df.rename(columns=rename)
    keep = [c for c in COL_MAP.values() if c in out.columns]
    out = out[keep].copy()

    if "obs_value" in out.columns:
        out["obs_value"] = pd.to_numeric(
            pd.Series(out["obs_value"], index=out.index), errors="coerce"
        )
    if "time_period" in out.columns:
        time_period = pd.Series(out["time_period"], index=out.index).astype(str).str.strip()
        time_period_date = pd.Series(
            pd.to_datetime(time_period + "-01-01", errors="coerce"),
            index=out.index,
        )
        out["time_period"] = time_period
        out["time_period_date"] = time_period_date
        cfg = dict(getattr(ctx.source, "config", None) or {})
        start_year = cfg.get("start_year")
        if start_year is not None:
            out = out.loc[time_period_date.dt.year >= int(start_year)].copy()

    return pd.DataFrame(out.reset_index(drop=True))

"""UN PSData transform — payload as json.dumps string."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd


def _record_id_fallback(dataset_id: Any, record_obj: dict) -> str:
    raw = f"{dataset_id}|{json.dumps(record_obj, sort_keys=True, default=str)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def transform(ctx: Any, raw: pd.DataFrame | bytes) -> pd.DataFrame:
    if isinstance(raw, (bytes, bytearray)):
        payload = json.loads(raw.decode("utf-8"))
        raw = pd.DataFrame(payload.get("data") or payload.get("records") or [])

    df = raw.copy()
    if df.empty:
        return pd.DataFrame(columns=["dataset_id", "record_id", "record_payload"])

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        dataset_id = row.get("dataset_id")
        record = row.get("record") if "record" in row.index else row.to_dict()
        if isinstance(record, str):
            try:
                record_obj = json.loads(record)
            except json.JSONDecodeError:
                record_obj = {"raw": record}
        else:
            record_obj = record if isinstance(record, dict) else {"value": record}
        record_id = (
            record_obj.get("id")
            or record_obj.get("record_id")
            or record_obj.get("Id")
            or _record_id_fallback(dataset_id, record_obj)
        )
        rows.append(
            {
                "dataset_id": str(dataset_id),
                "record_id": str(record_id),
                "record_payload": json.dumps(record_obj, sort_keys=True, default=str),
            }
        )
    return pd.DataFrame(rows)

"""Row validation and quarantine split."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class ValidationResult:
    good: pd.DataFrame
    quarantine: pd.DataFrame
    errors: list[str]


def validate_dataframe(
    df: pd.DataFrame,
    *,
    required: list[str] | None = None,
    min_rows: int | None = None,
    skip_min_rows: bool = False,
) -> ValidationResult:
    """Split invalid rows; collect run-level errors."""
    errors: list[str] = []
    required = required or []
    if df is None or df.empty:
        if min_rows and not skip_min_rows and min_rows > 0:
            errors.append(f"expected at least {min_rows} rows, got 0")
        empty = df if df is not None else pd.DataFrame()
        return ValidationResult(good=empty.copy(), quarantine=empty.copy(), errors=errors)

    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        errors.append(f"missing required columns: {missing_cols}")
        q = df.copy()
        q["_quarantine_reason"] = f"missing columns: {missing_cols}"
        return ValidationResult(good=df.iloc[0:0].copy(), quarantine=q, errors=errors)

    mask = pd.Series(True, index=df.index)
    reasons = pd.Series("", index=df.index, dtype=object)
    for col in required:
        bad = df[col].isna() | (df[col].astype(str).str.strip() == "")
        reasons = reasons.mask(bad & (reasons == ""), f"null/empty {col}")
        mask &= ~bad

    good = df.loc[mask].copy()
    quarantine = df.loc[~mask].copy()
    if not quarantine.empty:
        quarantine["_quarantine_reason"] = reasons.loc[~mask].values

    if min_rows is not None and not skip_min_rows and len(good) < min_rows:
        errors.append(f"expected at least {min_rows} good rows, got {len(good)}")

    return ValidationResult(good=good, quarantine=quarantine, errors=errors)

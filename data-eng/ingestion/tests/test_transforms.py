"""Source transform tests using fixtures."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from ingestion.sources.hdx_wfp_prices import transform as hdx_t
from ingestion.sources.un_psdata import transform as un_t
from ingestion.sources.wb_pink_sheet import transform as pink_t
from ingestion.sources.wb_rtfp import transform as rtfp_t
from ingestion.sources.world_bank_data360_harmonized_country_poverty import (
    transform as hcp_t,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture
def ctx() -> SimpleNamespace:
    return SimpleNamespace(source=SimpleNamespace(name="test"), settings=SimpleNamespace())


def test_hdx_skips_hxl_and_types(ctx) -> None:
    raw = (FIXTURES / "hdx_wfp_nga_sample.csv").read_bytes()
    df = hdx_t.transform(ctx, raw)
    assert len(df) == 50
    assert "date" in df.columns
    assert not str(df.iloc[0]["date"]).startswith("#")
    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    assert "price" in df.columns


def test_pink_sheet_melt_2x3(ctx) -> None:
    raw = (FIXTURES / "pink_sheet_sample.xlsx").read_bytes()
    df = pink_t.transform(ctx, raw)
    assert len(df) == 6  # 2 series x 3 months
    assert set(df.columns) >= {"period", "period_date", "series", "unit", "value"}
    assert set(df["series"].unique()) == {"Crude oil, average", "Cocoa"}
    assert set(df["period"].unique()) == {"2024M01", "2024M02", "2024M03"}
    assert df["value"].dtype == float or pd.api.types.is_float_dtype(df["value"])


def test_rtfp_columns(ctx) -> None:
    from ingestion.sources.wb_rtfp.extract import extract

    settings = SimpleNamespace(
        offline_fixtures=True,
        package_root=FIXTURES.parent.parent,
    )
    source = SimpleNamespace(url="https://example.test", config={})
    rctx = SimpleNamespace(settings=settings, source=source, limit=None)
    frames = list(extract(rctx))
    df = rtfp_t.transform(ctx, frames[0])
    assert {"country", "market", "date", "commodity"}.issubset(df.columns)
    assert len(df) == 5


def test_un_payload_json_dumps(ctx) -> None:
    import json

    payload = json.loads((FIXTURES / "un_page1.json").read_text(encoding="utf-8"))
    raw = pd.DataFrame(payload["data"])
    df = un_t.transform(ctx, raw)
    assert "record_payload" in df.columns
    parsed = json.loads(df.iloc[0]["record_payload"])
    assert isinstance(parsed, dict)
    assert df.iloc[0]["dataset_id"] == "DS1"


def test_world_bank_data360_hcp_normalize_and_optional_year_filter(ctx) -> None:
    from ingestion.sources.world_bank_data360_harmonized_country_poverty.extract import (
        extract,
    )

    settings = SimpleNamespace(
        offline_fixtures=True,
        package_root=FIXTURES.parent.parent,
    )
    source = SimpleNamespace(
        url="https://data360api.worldbank.org/data360/data",
        config={"start_year": 2020},
    )
    rctx = SimpleNamespace(settings=settings, source=source, limit=None)
    frames = list(extract(rctx))
    tctx = SimpleNamespace(source=source)
    df = hcp_t.transform(tctx, frames[0])
    assert {"database_id", "indicator", "ref_area", "time_period", "obs_value"}.issubset(
        df.columns
    )
    assert len(df) >= 1
    years = pd.Series(df["time_period_date"], index=df.index).dt.year
    assert bool((years >= 2020).all())
    raw_bytes = (
        FIXTURES / "world_bank_data360_harmonized_country_poverty_sample.json"
    ).read_bytes()
    df2 = hcp_t.transform(tctx, raw_bytes)
    assert len(df2) == len(df)

    # No start_year → keep all fixture years
    tctx_all = SimpleNamespace(source=SimpleNamespace(config={}))
    df_all = hcp_t.transform(tctx_all, frames[0])
    assert len(df_all) >= len(df)

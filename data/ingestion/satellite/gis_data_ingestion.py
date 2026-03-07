"""
Load GIS CSV (world countries and USA states lat/long) and save as a table in the local DB.
Uses engine_connector for the connection. Run from repo root:
  python data/ingestion/satellite/gis_data_ingestion.py
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = REPO_ROOT / "data" / "local" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import pandas as pd
from engine_connector import get_engine  # type: ignore[import-untyped]

# Path to the CSV (relative to repo root or this file)
CSV_PATH = REPO_ROOT / "data" / "ingestion" / "satellite" / "GIS" / "world_country_and_usa_states_latitude_and_longitude_values.csv"
TABLE_NAME = "world_country_usa_states_latlong"


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    engine = get_engine()

    with engine.begin() as conn:
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False, method="multi")

    print(f"Loaded {len(df)} rows into table '{TABLE_NAME}' in datateam_local.")


if __name__ == "__main__":
    main()

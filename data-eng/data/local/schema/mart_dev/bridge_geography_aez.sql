CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.bridge_geography_aez (
  geography_aez_bridge_key TEXT,
  geography_key TEXT,
  aez_key TEXT,
  aez_code TEXT,
  aez_name TEXT,
  aez_version TEXT,
  aez_source TEXT,
  source_natural_key TEXT,
  loaded_at TIMESTAMPTZ
);

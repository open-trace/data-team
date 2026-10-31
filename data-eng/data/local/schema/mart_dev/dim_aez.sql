CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_aez (
  aez_key TEXT,
  aez_code TEXT,
  aez_name TEXT,
  aez_version TEXT,
  aez_source TEXT,
  loaded_at TIMESTAMPTZ
);

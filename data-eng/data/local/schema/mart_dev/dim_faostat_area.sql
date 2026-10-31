CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_faostat_area (
  faostat_area_key TEXT,
  area_code TEXT,
  area_code_m49 TEXT,
  country_name TEXT,
  country_iso2 TEXT,
  country_iso3 TEXT,
  match_method TEXT,
  is_aggregate_area BOOLEAN,
  aggregate_reason TEXT,
  loaded_at TIMESTAMPTZ
);

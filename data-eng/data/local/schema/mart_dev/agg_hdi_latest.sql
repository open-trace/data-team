CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.agg_hdi_latest (
  agg_hdi_latest_key TEXT,
  geography_key TEXT,
  date_key TEXT,
  country_name TEXT,
  country_iso3 TEXT,
  latest_year BIGINT,
  hdi_value DOUBLE PRECISION,
  source_key TEXT,
  loaded_at TIMESTAMPTZ
);

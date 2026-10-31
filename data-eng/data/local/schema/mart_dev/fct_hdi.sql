CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_hdi (
  hdi_key TEXT,
  geography_key TEXT,
  geo_level TEXT,
  country_iso3 TEXT,
  source_key TEXT,
  tier BIGINT,
  data_level TEXT,
  geo_scope TEXT,
  place_scope JSONB,
  metric TEXT,
  source_id TEXT,
  year BIGINT,
  date_key TEXT,
  as_of_date DATE,
  as_of_date_basis TEXT,
  value DOUBLE PRECISION,
  unit TEXT,
  source_natural_key TEXT,
  loaded_at TIMESTAMPTZ
);

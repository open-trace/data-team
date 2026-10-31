CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_humanitarian (
  humanitarian_key TEXT,
  geography_key TEXT,
  geo_level TEXT,
  country_iso3 TEXT,
  item_key TEXT,
  element_key TEXT,
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
  unit TEXT,
  value DOUBLE PRECISION,
  source_natural_key TEXT,
  loaded_at TIMESTAMPTZ
);

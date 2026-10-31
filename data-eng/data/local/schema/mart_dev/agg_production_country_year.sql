CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.agg_production_country_year (
  agg_production_country_year_key TEXT,
  geography_key TEXT,
  date_key TEXT,
  country_iso3 TEXT,
  country_name TEXT,
  product_key TEXT,
  year BIGINT,
  source_key TEXT,
  area_harvested DOUBLE PRECISION,
  production_qty DOUBLE PRECISION,
  yield_recomputed DOUBLE PRECISION,
  row_count BIGINT,
  loaded_at TIMESTAMPTZ
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.agg_production_country_season (
  agg_production_country_season_key TEXT,
  geography_key TEXT,
  season_key TEXT,
  date_key TEXT,
  country_name TEXT,
  product_key TEXT,
  harvest_year BIGINT,
  source_key TEXT,
  area_harvested DOUBLE PRECISION,
  production_qty DOUBLE PRECISION,
  yield_recomputed DOUBLE PRECISION,
  fnid_count BIGINT,
  loaded_at TIMESTAMPTZ
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.agg_production_annual (
  geo_key TEXT,
  country_code TEXT,
  country_name TEXT,
  time_key BIGINT,
  product_key TEXT,
  product_name TEXT,
  source_key TEXT,
  source_name TEXT,
  record_count BIGINT,
  total_production_qty DOUBLE PRECISION,
  avg_production_qty DOUBLE PRECISION,
  total_area NUMERIC,
  avg_yield NUMERIC
);

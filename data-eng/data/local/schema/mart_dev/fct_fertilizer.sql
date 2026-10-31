CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_fertilizer (
  fertilizer_key TEXT,
  geo_key TEXT,
  time_key BIGINT,
  source_key TEXT,
  product_key TEXT,
  unit_key TEXT,
  quantity NUMERIC,
  source_record_id TEXT
);

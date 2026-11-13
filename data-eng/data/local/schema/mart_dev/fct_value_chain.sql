CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_value_chain (
  value_chain_key TEXT,
  geo_key TEXT,
  time_key BIGINT,
  source_key TEXT,
  product_key TEXT,
  unit_key TEXT,
  value_share DOUBLE PRECISION,
  source_record_id TEXT
);

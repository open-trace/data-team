CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_land_use (
  land_use_fact_key TEXT,
  geo_key TEXT,
  time_key BIGINT,
  source_key TEXT,
  land_use_key TEXT,
  area DOUBLE PRECISION,
  source_record_id TEXT
);

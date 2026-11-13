CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_land_degradation (
  land_degradation_key TEXT,
  geo_key TEXT,
  time_key BIGINT,
  source_key TEXT,
  unit_key TEXT,
  pct_degraded NUMERIC,
  upper_bound NUMERIC,
  lower_bound NUMERIC,
  source_record_id TEXT
);

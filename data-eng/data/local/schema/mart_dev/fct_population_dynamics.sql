CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_population_dynamics (
  population_fact_key TEXT,
  geo_key TEXT,
  time_key BIGINT,
  source_key TEXT,
  population_count NUMERIC,
  source_record_id TEXT
);

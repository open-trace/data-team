CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.agg_food_security_monthly (
  geo_key TEXT,
  country_code TEXT,
  country_name TEXT,
  time_key BIGINT,
  classification_key TEXT,
  phase_name TEXT,
  phase_number BIGINT,
  scenario_key TEXT,
  scenario_name TEXT,
  record_count BIGINT,
  total_population_affected NUMERIC,
  avg_population_affected NUMERIC,
  avg_pct_phase3 NUMERIC,
  avg_pct_phase4 NUMERIC,
  avg_pct_phase5 NUMERIC
);

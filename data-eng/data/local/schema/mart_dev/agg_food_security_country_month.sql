CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.agg_food_security_country_month (
  agg_food_security_country_month_key TEXT,
  geography_key TEXT,
  date_key TEXT,
  country_name TEXT,
  country_iso2 TEXT,
  measure_type TEXT,
  year BIGINT,
  month BIGINT,
  source_key TEXT,
  value_sum DOUBLE PRECISION,
  pct_phase3_avg DOUBLE PRECISION,
  pct_phase4_avg DOUBLE PRECISION,
  pct_phase5_avg DOUBLE PRECISION,
  unit_count BIGINT,
  loaded_at TIMESTAMPTZ
);

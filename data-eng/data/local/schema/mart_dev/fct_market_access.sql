CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_market_access (
  market_access_key TEXT,
  geo_key TEXT,
  time_key BIGINT,
  source_key TEXT,
  market_key TEXT,
  audit_key TEXT,
  access_index DOUBLE PRECISION,
  distance_to_market NUMERIC,
  travel_time_minutes NUMERIC,
  purchase_frequency TEXT,
  outlet_choice_cleanliness BIGINT,
  outlet_choice_hygiene BIGINT,
  outlet_choice_quality BIGINT,
  outlet_choice_price BIGINT,
  years_operating BIGINT,
  operating_days_per_week BIGINT,
  avg_daily_sales_volume NUMERIC,
  primary_supply_source TEXT,
  source_record_id TEXT
);

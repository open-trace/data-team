CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.agg_food_balance_country_year (
  agg_food_balance_country_year_key TEXT,
  geography_key TEXT,
  date_key TEXT,
  country_name TEXT,
  product_key TEXT,
  year BIGINT,
  source_key TEXT,
  production_sum DOUBLE PRECISION,
  imports_sum DOUBLE PRECISION,
  exports_sum DOUBLE PRECISION,
  food_sum DOUBLE PRECISION,
  feed_sum DOUBLE PRECISION,
  losses_sum DOUBLE PRECISION,
  row_count BIGINT,
  loaded_at TIMESTAMPTZ
);

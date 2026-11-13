CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.agg_prices_country_month (
  agg_prices_country_month_key TEXT,
  geography_key TEXT,
  date_key TEXT,
  country_name TEXT,
  product_key TEXT,
  price_type TEXT,
  price_source TEXT,
  source_key TEXT,
  year BIGINT,
  month BIGINT,
  price_avg DOUBLE PRECISION,
  common_unit_price_avg DOUBLE PRECISION,
  market_count BIGINT,
  loaded_at TIMESTAMPTZ
);

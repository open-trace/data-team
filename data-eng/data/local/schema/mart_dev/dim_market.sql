CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_market (
  market_key TEXT,
  country TEXT,
  admin_1 TEXT,
  market_name TEXT,
  loaded_at TIMESTAMPTZ
);

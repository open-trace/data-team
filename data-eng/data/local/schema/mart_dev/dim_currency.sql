CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_currency (
  currency_key TEXT,
  currency_code TEXT,
  currency_name TEXT
);

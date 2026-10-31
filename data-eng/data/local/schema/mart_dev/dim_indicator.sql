CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_indicator (
  indicator_key TEXT,
  indicator_name TEXT,
  loaded_at TIMESTAMPTZ
);

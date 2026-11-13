CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_livestock (
  livestock_key TEXT,
  species TEXT,
  loaded_at TIMESTAMPTZ
);

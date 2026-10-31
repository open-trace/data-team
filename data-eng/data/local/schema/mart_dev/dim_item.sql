CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_item (
  item_key TEXT,
  item_name TEXT,
  item_code TEXT,
  loaded_at TIMESTAMPTZ
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_product (
  product_key TEXT,
  item_code TEXT,
  product_name TEXT,
  cpcv2 TEXT,
  loaded_at TIMESTAMPTZ
);

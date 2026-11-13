CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_element (
  element_key TEXT,
  element_name TEXT,
  element_code TEXT,
  loaded_at TIMESTAMPTZ
);

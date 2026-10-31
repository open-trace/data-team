CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_sex (
  sex_key TEXT,
  sex_label TEXT,
  loaded_at TIMESTAMPTZ
);

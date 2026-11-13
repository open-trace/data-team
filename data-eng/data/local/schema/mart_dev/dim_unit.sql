CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_unit (
  unit_key TEXT,
  unit_code TEXT,
  unit_type TEXT,
  loaded_at TIMESTAMPTZ
);

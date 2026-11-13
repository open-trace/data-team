CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_land_use (
  land_use_key TEXT,
  land_use_code TEXT,
  land_use_class TEXT,
  description TEXT
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_water (
  water_key TEXT,
  water_source TEXT,
  water_access_type TEXT,
  description TEXT
);

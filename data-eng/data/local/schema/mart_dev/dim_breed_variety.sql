CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_breed_variety (
  breed_variety_key TEXT,
  variety_code TEXT,
  variety_name TEXT,
  species TEXT,
  description TEXT,
  valid_from DATE,
  valid_to DATE,
  is_current BOOLEAN
);

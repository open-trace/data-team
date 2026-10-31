CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_pest (
  pest_key TEXT,
  pest_natural_key TEXT,
  pest_name TEXT,
  pest_type TEXT,
  target_crop_livestock TEXT,
  description TEXT,
  valid_from DATE,
  valid_to DATE,
  is_current BOOLEAN
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_soil (
  soil_key TEXT,
  depth_layer TEXT,
  depth_min_cm BIGINT,
  depth_max_cm BIGINT,
  description TEXT
);

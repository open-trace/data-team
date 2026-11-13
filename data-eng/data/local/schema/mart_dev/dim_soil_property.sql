CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_soil_property (
  soil_property_key TEXT,
  soil_property TEXT,
  depth TEXT,
  loaded_at TIMESTAMPTZ
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.agg_soil_admin (
  agg_soil_admin_key TEXT,
  geography_key TEXT,
  country_name TEXT,
  admin_1_name TEXT,
  soil_property TEXT,
  depth TEXT,
  source_key TEXT,
  value_avg DOUBLE PRECISION,
  point_count BIGINT,
  loaded_at TIMESTAMPTZ
);

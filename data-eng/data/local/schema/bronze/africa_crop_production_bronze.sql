CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.africa_crop_production_bronze (
  country_name TEXT,
  country_code_m49 TEXT,
  crop_name TEXT,
  indicator_name TEXT,
  observation_year BIGINT,
  unit TEXT,
  indicator_value DOUBLE PRECISION,
  ingested_at TIMESTAMPTZ
);

CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.copernicus_climate_raw_era5_stats_2023_06 (
  time TIMESTAMP,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  number BIGINT,
  step TEXT,
  surface DOUBLE PRECISION,
  valid_time TIMESTAMP,
  t2m DOUBLE PRECISION
);

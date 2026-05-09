CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.africa_nasa_power_hourly_bronze (
  ingestion_id TEXT,
  source TEXT,
  dataset_code TEXT,
  country_code TEXT,
  admin_region TEXT,
  longitude DOUBLE PRECISION,
  latitude DOUBLE PRECISION,
  elevation DOUBLE PRECISION,
  observation_time TIMESTAMPTZ,
  par_total DOUBLE PRECISION,
  shortwave_irradiance DOUBLE PRECISION,
  uva_irradiance DOUBLE PRECISION,
  uvb_irradiance DOUBLE PRECISION,
  fetched_at TIMESTAMPTZ
);

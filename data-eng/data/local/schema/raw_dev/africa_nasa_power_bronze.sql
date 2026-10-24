CREATE TABLE IF NOT EXISTS africa_nasa_power_bronze (
  ingestion_id TEXT,
  country_code TEXT,
  country_name TEXT,
  admin_region TEXT,
  fetched_at TIMESTAMPTZ,
  elevation_meters DOUBLE PRECISION,
  par_solar_at_noon DOUBLE PRECISION,
  shortwave_irradiance_at_noon DOUBLE PRECISION,
  uva_radiation_at_noon DOUBLE PRECISION,
  uvb_radiation_at_noon DOUBLE PRECISION,
  processed_at TIMESTAMPTZ
);

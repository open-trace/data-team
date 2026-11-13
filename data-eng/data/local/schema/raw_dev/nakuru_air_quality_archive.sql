CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.nakuru_air_quality_archive (
  sensor_id BIGINT,
  sensor_type TEXT,
  location BIGINT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  timestamp TEXT,
  pm10 DOUBLE PRECISION,
  pm2_5 DOUBLE PRECISION,
  humidity_pct DOUBLE PRECISION,
  temp_c DOUBLE PRECISION,
  source_archive TEXT
);

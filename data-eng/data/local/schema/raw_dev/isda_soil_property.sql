CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.isda_soil_property (
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  value DOUBLE PRECISION,
  depth TEXT,
  property TEXT,
  ingested_at TIMESTAMPTZ
);

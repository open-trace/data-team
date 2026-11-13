CREATE TABLE IF NOT EXISTS isda_soil_bulk_master_bronze (
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  value DOUBLE PRECISION,
  depth TEXT,
  property TEXT,
  ingested_at TIMESTAMPTZ
);

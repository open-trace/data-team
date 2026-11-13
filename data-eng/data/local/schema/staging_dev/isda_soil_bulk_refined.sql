CREATE TABLE IF NOT EXISTS isda_soil_bulk_refined (
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  depth TEXT,
  property_slug TEXT,
  calculated_value DOUBLE PRECISION,
  unit TEXT,
  bronze_ingestion_time TIMESTAMPTZ,
  silver_processed_at TIMESTAMPTZ
);

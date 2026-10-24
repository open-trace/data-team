CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.climatewatch_climate_health_impacts (
  id BIGINT,
  location TEXT,
  iso_code2 TEXT,
  model TEXT,
  scenario TEXT,
  category TEXT,
  subcategory TEXT,
  indicator TEXT,
  composite_name TEXT,
  unit TEXT,
  definition TEXT,
  year BIGINT,
  value DOUBLE PRECISION,
  ingested_at TIMESTAMPTZ
);

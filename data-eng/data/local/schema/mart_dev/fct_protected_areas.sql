CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_protected_areas (
  protected_area_key TEXT,
  geography_key TEXT,
  geo_level TEXT,
  country_iso3 TEXT,
  source_key TEXT,
  tier BIGINT,
  data_level TEXT,
  geo_scope TEXT,
  place_scope JSONB,
  metric TEXT,
  source_id TEXT,
  as_of_date DATE,
  as_of_date_basis TEXT,
  value DOUBLE PRECISION,
  unit TEXT,
  protected_area_name TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  geometry_wkt TEXT,
  source_natural_key TEXT,
  loaded_at TIMESTAMPTZ
);

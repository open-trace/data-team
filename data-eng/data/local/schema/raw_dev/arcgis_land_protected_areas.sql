CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.arcgis_land_protected_areas (
  objectid BIGINT,
  name TEXT,
  count BIGINT,
  analysisarea DOUBLE PRECISION,
  geometry_wkt TEXT
);

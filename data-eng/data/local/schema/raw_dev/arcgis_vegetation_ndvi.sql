CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.arcgis_vegetation_ndvi (
  objectid BIGINT,
  grid_id TEXT,
  mean DOUBLE PRECISION,
  mean_1 DOUBLE PRECISION,
  shape__area DOUBLE PRECISION,
  shape__length DOUBLE PRECISION,
  geometry_wkt TEXT
);

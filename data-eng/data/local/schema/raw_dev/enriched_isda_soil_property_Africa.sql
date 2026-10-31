CREATE SCHEMA IF NOT EXISTS "raw_dev";
CREATE TABLE IF NOT EXISTS "raw_dev"."enriched_isda_soil_property_Africa" (
  "value" DOUBLE PRECISION,
  "depth" TEXT,
  "property" TEXT,
  "lon_wgs84" DOUBLE PRECISION,
  "lat_wgs84" DOUBLE PRECISION,
  "city" TEXT,
  "country" TEXT,
  "id" BIGINT
);

CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."isda_soil_bulk_master" (
  "latitude" DOUBLE PRECISION,
  "longitude" DOUBLE PRECISION,
  "value" DOUBLE PRECISION,
  "depth" TEXT,
  "property" TEXT
);

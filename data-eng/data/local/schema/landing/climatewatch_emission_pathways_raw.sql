CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."climatewatch_emission_pathways_raw" (
  "id" BIGINT,
  "location" TEXT,
  "iso_code2" TEXT,
  "model" TEXT,
  "scenario" TEXT,
  "category" TEXT,
  "subcategory" TEXT,
  "indicator" TEXT,
  "composite_name" TEXT,
  "unit" TEXT,
  "definition" TEXT,
  "emissions" TEXT
);

CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."africa_crop_production_full_melted" (
  "area_code" BIGINT,
  "area_code_m49" TEXT,
  "area" TEXT,
  "item_code" BIGINT,
  "item_code_cpc" TEXT,
  "item" TEXT,
  "element_code" BIGINT,
  "element" TEXT,
  "unit" TEXT,
  "year" BIGINT,
  "indicator_value" DOUBLE PRECISION
);

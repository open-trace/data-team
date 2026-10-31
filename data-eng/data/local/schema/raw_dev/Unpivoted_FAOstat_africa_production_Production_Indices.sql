CREATE SCHEMA IF NOT EXISTS "raw_dev";
CREATE TABLE IF NOT EXISTS "raw_dev"."Unpivoted_FAOstat_africa_production_Production Indices" (
  "area_code" BIGINT,
  "area_code_m49" TEXT,
  "area" TEXT,
  "item_code" BIGINT,
  "item_code_cpc" TEXT,
  "item" TEXT,
  "element_code" BIGINT,
  "element" TEXT,
  "year" BIGINT,
  "Unit" DOUBLE PRECISION,
  "value" DOUBLE PRECISION
);

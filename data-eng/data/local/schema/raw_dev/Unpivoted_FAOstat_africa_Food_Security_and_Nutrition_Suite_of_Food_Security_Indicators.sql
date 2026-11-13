CREATE SCHEMA IF NOT EXISTS "raw_dev";
CREATE TABLE IF NOT EXISTS "raw_dev"."Unpivoted_FAOstat_africa_Food_Security_a_5a0c278562fa" (
  "area_code" BIGINT,
  "area_code_m49" TEXT,
  "area" TEXT,
  "item_code" TEXT,
  "item" TEXT,
  "element_code" BIGINT,
  "element" TEXT,
  "unit" TEXT,
  "period_key" TEXT,
  "start_year" BIGINT,
  "end_year" BIGINT,
  "period_type" TEXT,
  "value" DOUBLE PRECISION
);

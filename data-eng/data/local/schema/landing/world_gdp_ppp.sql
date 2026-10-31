CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."world_gdp_ppp" (
  "country" TEXT,
  "alpha_3_code" TEXT,
  "year" TEXT,
  "gdp_per_capita" TEXT
);

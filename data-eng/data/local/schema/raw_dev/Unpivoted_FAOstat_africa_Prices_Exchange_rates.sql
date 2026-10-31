CREATE SCHEMA IF NOT EXISTS "raw_dev";
CREATE TABLE IF NOT EXISTS "raw_dev"."Unpivoted_FAOstat_africa_Prices_Exchange_rates" (
  "Area_Code" TEXT,
  "Area_Code_M49" TEXT,
  "Area" TEXT,
  "Element_Code" TEXT,
  "Element" TEXT,
  "ISO_Currency_Code" TEXT,
  "Currency" TEXT,
  "Months_Code" TEXT,
  "Months" TEXT,
  "year" BIGINT,
  "Unit" TEXT,
  "value" TEXT
);

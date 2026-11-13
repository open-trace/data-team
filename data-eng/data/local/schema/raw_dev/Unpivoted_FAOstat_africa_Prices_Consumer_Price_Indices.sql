CREATE SCHEMA IF NOT EXISTS "raw_dev";
CREATE TABLE IF NOT EXISTS "raw_dev"."Unpivoted_FAOstat_africa_Prices_Consumer_Price_Indices" (
  "Area_Code" TEXT,
  "Area_Code_M49" TEXT,
  "Area" TEXT,
  "Item_Code" TEXT,
  "Item" TEXT,
  "Element_Code" TEXT,
  "Element" TEXT,
  "Months_Code" TEXT,
  "Months" TEXT,
  "year" BIGINT,
  "Unit" TEXT,
  "value" TEXT
);

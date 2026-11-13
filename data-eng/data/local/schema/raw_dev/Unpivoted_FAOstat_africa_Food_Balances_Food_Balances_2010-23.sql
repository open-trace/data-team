CREATE SCHEMA IF NOT EXISTS "raw_dev";
CREATE TABLE IF NOT EXISTS "raw_dev"."Unpivoted_FAOstat_africa_Food Balances_Food_Balances_2010-23" (
  "Area_Code" TEXT,
  "Area_Code_M49" TEXT,
  "Area" TEXT,
  "Item_Code" TEXT,
  "Item_Code_FBS" TEXT,
  "Item" TEXT,
  "Element_Code" TEXT,
  "Element" TEXT,
  "year" BIGINT,
  "Unit" TEXT,
  "value" TEXT
);

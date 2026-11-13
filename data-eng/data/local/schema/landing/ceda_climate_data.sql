CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."ceda_climate_data" (
  "type" TEXT,
  "id" TEXT,
  "properties_title" TEXT,
  "properties_identifier" TEXT,
  "properties_links_search" JSONB,
  "properties_links_related" JSONB,
  "properties_date" TEXT
);

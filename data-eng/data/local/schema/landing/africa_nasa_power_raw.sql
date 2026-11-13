CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."africa_nasa_power_raw" (
  "ingestion_id" TEXT,
  "source" TEXT,
  "dataset_code" TEXT,
  "country_code" TEXT,
  "admin_region" TEXT,
  "fetched_at" TIMESTAMPTZ,
  "payload" JSONB,
  "status" TEXT
);

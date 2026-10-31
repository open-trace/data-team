CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."openaire_projects_raw" (
  "ingestion_id" TEXT,
  "payload" TEXT,
  "fetched_at" TIMESTAMPTZ
);

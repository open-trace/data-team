CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."openaire_data_sources_raw" (
  "ingestion_id" TEXT,
  "source" TEXT,
  "dataset" TEXT,
  "endpoint" TEXT,
  "page_offset" BIGINT,
  "http_status" BIGINT,
  "fetched_at" TIMESTAMPTZ,
  "payload" TEXT,
  "status" TEXT
);

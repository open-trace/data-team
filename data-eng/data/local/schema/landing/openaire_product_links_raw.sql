CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."openaire_product_links_raw" (
  "ingestion_id" TEXT,
  "source_pair" TEXT,
  "payload" TEXT,
  "fetched_at" TIMESTAMPTZ,
  "status" TEXT
);

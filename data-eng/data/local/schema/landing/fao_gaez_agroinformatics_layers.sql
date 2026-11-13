CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."fao_gaez_agroinformatics_layers" (
  "page_content" TEXT,
  "status_code" BIGINT,
  "source_url" TEXT,
  "_ingested_at" TIMESTAMPTZ
);

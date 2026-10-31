CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."isric_full_metadata_catalog" (
  "uri" TEXT,
  "identifier" TEXT,
  "title" TEXT,
  "abstract" TEXT,
  "issued_date" TEXT,
  "temporal_coverage" TEXT,
  "spatial_polygon" TEXT,
  "license" TEXT,
  "all_keywords" TEXT,
  "download_links" TEXT,
  "thumbnail_url" TEXT
);

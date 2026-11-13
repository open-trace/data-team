CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."africa_geoportals_top_100" (
  "id" TEXT,
  "title" TEXT,
  "owner" TEXT,
  "type" TEXT,
  "modified" DATE,
  "tags" TEXT,
  "snippet" TEXT,
  "url" TEXT
);

CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."openaire_persons_raw" (
  "person_id" TEXT,
  "given_name" TEXT,
  "family_name" TEXT,
  "alt_names" TEXT,
  "biography" TEXT,
  "coauthor_count" BIGINT,
  "fetched_at" TEXT
);

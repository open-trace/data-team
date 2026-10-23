CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.openaire_agriculture_and_environment_Research_publications_Projects_bronze (
  ingestion_id TEXT,
  fetched_at TIMESTAMPTZ,
  project_id TEXT,
  project_code TEXT,
  acronym TEXT,
  title TEXT,
  start_date DATE,
  currency TEXT,
  total_cost DOUBLE PRECISION,
  funded_amount DOUBLE PRECISION,
  primary_funder_name TEXT,
  jurisdiction TEXT,
  funding_stream_desc TEXT
);

CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.openaire_agriculture_and_environment_Research_publications_Organizations_bronze (
  org_id TEXT,
  legal_name TEXT,
  short_name TEXT,
  website_url TEXT,
  country_code TEXT,
  country_name TEXT,
  alternative_names JSONB,
  pids JSONB,
  fetched_at TIMESTAMPTZ
);

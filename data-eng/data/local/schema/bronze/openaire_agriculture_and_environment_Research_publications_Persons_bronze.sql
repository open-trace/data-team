CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.openaire_agriculture_and_environment_Research_publications_Persons_bronze (
  person_id TEXT,
  given_name TEXT,
  family_name TEXT,
  biography TEXT,
  coauthor_count BIGINT,
  fetched_at TIMESTAMPTZ
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_person (
  person_key TEXT,
  person_id TEXT,
  given_name TEXT,
  family_name TEXT,
  biography TEXT,
  coauthor_count BIGINT,
  source_natural_key TEXT,
  loaded_at TIMESTAMPTZ
);

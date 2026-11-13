CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_organisation (
  organisation_key TEXT,
  organisation_id TEXT,
  legal_name TEXT,
  short_name TEXT,
  website_url TEXT,
  country_code TEXT,
  country_name TEXT,
  org_source TEXT,
  source_natural_key TEXT,
  loaded_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS openaire_organizations_bronze (
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

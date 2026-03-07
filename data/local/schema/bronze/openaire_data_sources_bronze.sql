CREATE TABLE IF NOT EXISTS openaire_data_sources_bronze (
  ingestion_id TEXT,
  fetched_at TIMESTAMPTZ,
  openaire_id TEXT,
  official_name TEXT,
  english_name TEXT,
  website_url TEXT,
  type TEXT,
  compatibility TEXT,
  subjects JSONB,
  issn_online TEXT,
  issn_printed TEXT
);

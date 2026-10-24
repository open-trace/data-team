CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.openaire_agriculture_and_environment_Research_publications_Product_links_bronze (
  openaire_id TEXT,
  title TEXT,
  entity_type TEXT,
  pub_date TEXT,
  publisher TEXT,
  language TEXT,
  rel_name TEXT,
  target_id TEXT,
  target_type TEXT,
  fetched_at TIMESTAMPTZ
);

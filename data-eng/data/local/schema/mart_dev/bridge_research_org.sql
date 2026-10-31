CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.bridge_research_org (
  research_org_bridge_key TEXT,
  openaire_id TEXT,
  organisation_key TEXT,
  target_id TEXT,
  rel_name TEXT,
  target_type TEXT,
  source_natural_key TEXT,
  loaded_at TIMESTAMPTZ
);

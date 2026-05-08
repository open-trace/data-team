CREATE TABLE IF NOT EXISTS openaire_full_bronze (
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

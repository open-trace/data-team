CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_research_project (
  research_project_key TEXT,
  project_id TEXT,
  project_code TEXT,
  acronym TEXT,
  project_title TEXT,
  start_date DATE,
  currency TEXT,
  total_cost DOUBLE PRECISION,
  funded_amount DOUBLE PRECISION,
  primary_funder_name TEXT,
  jurisdiction TEXT,
  funding_stream_desc TEXT,
  source_natural_key TEXT,
  loaded_at TIMESTAMPTZ
);

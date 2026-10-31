CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_source (
  source_key TEXT,
  source_natural_key TEXT,
  organisation_name TEXT,
  tier BIGINT,
  default_data_level TEXT,
  producer_scale TEXT,
  loaded_at TIMESTAMPTZ
);

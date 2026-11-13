CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_disease (
  disease_key TEXT,
  disease_or_hazard TEXT,
  disease_family TEXT,
  loaded_at TIMESTAMPTZ
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_classification (
  classification_key TEXT,
  phase_code TEXT,
  phase_name TEXT,
  classification_scale TEXT,
  loaded_at TIMESTAMPTZ
);

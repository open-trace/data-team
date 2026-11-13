CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_pesticide (
  pesticide_key TEXT,
  geo_key TEXT,
  time_key BIGINT,
  source_key TEXT,
  product_key TEXT,
  pest_key TEXT,
  unit_key TEXT,
  quantity NUMERIC,
  application_frequency NUMERIC,
  eiq_score NUMERIC,
  eiq_field_use NUMERIC,
  treatment_misuse_flag BOOLEAN,
  source_record_id TEXT
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_technology (
  technology_fact_key TEXT,
  geo_key TEXT,
  time_key BIGINT,
  source_key TEXT,
  organisation_key TEXT,
  technology_key TEXT,
  unit_key TEXT,
  audit_key TEXT,
  machinery_stock NUMERIC,
  rd_expenditure NUMERIC,
  rd_intensity DOUBLE PRECISION,
  source_record_id TEXT
);

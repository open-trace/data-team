CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_research_expenditure (
  research_exp_key TEXT,
  geo_key TEXT,
  time_key BIGINT,
  source_key TEXT,
  organisation_key TEXT,
  unit_key TEXT,
  expenditure_amount NUMERIC,
  source_record_id TEXT
);

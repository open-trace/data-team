CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.fct_researchers (
  researcher_fact_key TEXT,
  geo_key TEXT,
  time_key BIGINT,
  source_key TEXT,
  organisation_key TEXT,
  person_key TEXT,
  unit_key TEXT,
  researcher_count_fte NUMERIC,
  researcher_count_hc BIGINT,
  source_record_id TEXT
);

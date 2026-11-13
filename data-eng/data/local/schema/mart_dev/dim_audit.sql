CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_audit (
  audit_key TEXT,
  ingestion_timestamp TIMESTAMPTZ,
  source_system TEXT,
  source_file_name TEXT,
  batch_id TEXT,
  pipeline_run_id TEXT,
  processed_at TIMESTAMPTZ,
  record_hash TEXT,
  is_deleted BOOLEAN
);

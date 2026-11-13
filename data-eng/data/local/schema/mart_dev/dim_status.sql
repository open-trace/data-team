CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_status (
  status_key TEXT,
  status_code TEXT,
  status_name TEXT,
  status_type TEXT
);

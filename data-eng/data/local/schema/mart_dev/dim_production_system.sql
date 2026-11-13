CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_production_system (
  production_system_key TEXT,
  system_code TEXT,
  system_name TEXT,
  description TEXT
);

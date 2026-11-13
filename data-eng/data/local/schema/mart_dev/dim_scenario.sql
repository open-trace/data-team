CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_scenario (
  scenario_key TEXT,
  scenario_code TEXT,
  scenario_name TEXT,
  description TEXT,
  is_current BOOLEAN
);

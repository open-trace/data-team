CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_household (
  household_key TEXT,
  household_id TEXT,
  country TEXT,
  region TEXT,
  district TEXT,
  village_code TEXT,
  household_type TEXT,
  head_education_level TEXT,
  source_natural_key TEXT,
  loaded_at TIMESTAMPTZ
);

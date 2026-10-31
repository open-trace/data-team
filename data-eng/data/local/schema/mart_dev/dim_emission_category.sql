CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_emission_category (
  emission_category_key TEXT,
  category_code TEXT,
  category_name TEXT,
  parent_category TEXT,
  description TEXT
);

CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.africa_Human_development_index (
  country TEXT,
  alpha_3_code TEXT,
  year TEXT,
  index TEXT
);

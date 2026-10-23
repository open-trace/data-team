CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.africa_gross_domestic_product_purchasing_power_parity_bronze (
  country_name TEXT,
  country_code TEXT,
  observation_year BIGINT,
  gdp_per_capita_ppp DOUBLE PRECISION,
  ingested_at TIMESTAMPTZ
);

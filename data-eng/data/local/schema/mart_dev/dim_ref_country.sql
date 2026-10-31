CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_ref_country (
  country_name TEXT,
  country_iso2 TEXT,
  country_iso3 TEXT,
  population BIGINT,
  in_africa_scope BOOLEAN,
  m49_code TEXT,
  loaded_at TIMESTAMPTZ
);

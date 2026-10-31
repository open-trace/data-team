CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_geography (
  geography_key TEXT,
  geo_level TEXT,
  data_level TEXT,
  country_name TEXT,
  country_iso2 TEXT,
  country_iso3 TEXT,
  country_key TEXT,
  admin_1_name TEXT,
  admin_2_name TEXT,
  city_name TEXT,
  fnid TEXT,
  parent_geography_key TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  population BIGINT,
  capital_status TEXT,
  native_id TEXT,
  loaded_at TIMESTAMPTZ
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.agg_emissions_country_year (
  agg_emissions_country_year_key TEXT,
  geography_key TEXT,
  date_key TEXT,
  country_name TEXT,
  item_key TEXT,
  element_key TEXT,
  unit TEXT,
  year BIGINT,
  source_key TEXT,
  value_avg DOUBLE PRECISION,
  row_count BIGINT,
  loaded_at TIMESTAMPTZ
);

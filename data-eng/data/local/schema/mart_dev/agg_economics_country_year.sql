CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.agg_economics_country_year (
  agg_economics_country_year_key TEXT,
  geography_key TEXT,
  date_key TEXT,
  country_name TEXT,
  item_key TEXT,
  measurement_form TEXT,
  unit TEXT,
  year BIGINT,
  source_key TEXT,
  value_avg DOUBLE PRECISION,
  row_count BIGINT,
  loaded_at TIMESTAMPTZ
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_season (
  season_key TEXT,
  country TEXT,
  season_name TEXT,
  season_name_norm TEXT,
  start_month BIGINT,
  end_month BIGINT,
  crosses_year BOOLEAN,
  is_off_season BOOLEAN,
  n_rows BIGINT,
  loaded_at TIMESTAMPTZ
);

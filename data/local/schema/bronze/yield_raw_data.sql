CREATE TABLE IF NOT EXISTS yield_raw_data (
  fnid TEXT,
  country TEXT,
  country_code TEXT,
  admin_1 TEXT,
  admin_2 TEXT,
  product TEXT,
  season_name TEXT,
  planting_year BIGINT,
  planting_month BIGINT,
  harvest_year BIGINT,
  harvest_month BIGINT,
  crop_production_system TEXT,
  qc_flag BIGINT,
  area DOUBLE PRECISION,
  production DOUBLE PRECISION,
  yield DOUBLE PRECISION
);

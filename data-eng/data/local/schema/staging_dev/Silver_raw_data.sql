CREATE TABLE IF NOT EXISTS Silver_raw_data (
  country TEXT,
  country_code TEXT,
  product TEXT,
  season_name TEXT,
  planting_year BIGINT,
  planting_month BIGINT,
  harvest_year BIGINT,
  harvest_month BIGINT,
  area DOUBLE PRECISION,
  production DOUBLE PRECISION,
  yield DOUBLE PRECISION,
  f0_ TIMESTAMPTZ
);

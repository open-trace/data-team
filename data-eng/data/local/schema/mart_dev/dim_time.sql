CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_time (
  time_key BIGINT,
  date_actual DATE,
  year BIGINT,
  quarter BIGINT,
  month BIGINT,
  month_name TEXT,
  day_of_month BIGINT,
  day_of_week BIGINT,
  week_of_year BIGINT,
  is_weekend BOOLEAN,
  fiscal_year BIGINT,
  created_at TIMESTAMPTZ
);

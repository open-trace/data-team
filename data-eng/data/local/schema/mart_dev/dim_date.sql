CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_date (
  date_key TEXT,
  date_day DATE,
  year BIGINT,
  month BIGINT,
  quarter BIGINT,
  year_month TEXT
);

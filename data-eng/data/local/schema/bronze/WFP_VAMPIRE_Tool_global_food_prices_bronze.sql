CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.WFP_VAMPIRE_Tool_global_food_prices_bronze (
  adm0_id DOUBLE PRECISION,
  adm0_name TEXT,
  adm1_id BIGINT,
  adm1_name TEXT,
  mkt_id BIGINT,
  mkt_name TEXT,
  cm_id BIGINT,
  cm_name TEXT,
  cur_id DOUBLE PRECISION,
  cur_name TEXT,
  pt_id BIGINT,
  pt_name TEXT,
  um_id BIGINT,
  um_name TEXT,
  mp_month BIGINT,
  mp_year BIGINT,
  mp_price DOUBLE PRECISION,
  mp_commoditysource DOUBLE PRECISION
);

CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_trade_flow (
  trade_flow_key TEXT,
  flow_type TEXT,
  trade_type TEXT,
  description TEXT
);

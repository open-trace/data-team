CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.bridge_season_calendar (
  season_key TEXT,
  time_key BIGINT,
  is_typical_window BOOLEAN
);

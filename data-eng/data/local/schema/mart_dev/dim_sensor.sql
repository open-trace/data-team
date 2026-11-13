CREATE SCHEMA IF NOT EXISTS mart_dev;
CREATE TABLE IF NOT EXISTS mart_dev.dim_sensor (
  sensor_key TEXT,
  sensor_id TEXT,
  sensor_type TEXT,
  location_description TEXT
);

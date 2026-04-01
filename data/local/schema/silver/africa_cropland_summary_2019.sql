CREATE TABLE IF NOT EXISTS africa_cropland_summary_2019 (
  agroecological_zone TEXT,
  total_satellite_tiles BIGINT,
  total_cultivated_hectares DOUBLE PRECISION,
  avg_crop_intensity_percent DOUBLE PRECISION,
  processed_at TIMESTAMPTZ,
  data_origin TEXT
);

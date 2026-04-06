CREATE TABLE IF NOT EXISTS fao_rl_landuse (
  area_code DOUBLE PRECISION,
  area TEXT,
  element_code DOUBLE PRECISION,
  element TEXT,
  item_code DOUBLE PRECISION,
  item TEXT,
  year DOUBLE PRECISION,
  unit TEXT,
  value DOUBLE PRECISION,
  country_code TEXT,
  country_name TEXT,
  f0_ TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS fao_qcl (
  area_code TEXT,
  area TEXT,
  element_code TEXT,
  element TEXT,
  item_code TEXT,
  item TEXT,
  year TEXT,
  unit TEXT,
  value DOUBLE PRECISION,
  country_code TEXT,
  country_name TEXT,
  f0_ TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS fews_net_food_security_master (
  country TEXT,
  country_code TEXT,
  geographic_unit_name TEXT,
  unit_type TEXT,
  ipc_phase_value DOUBLE PRECISION,
  ipc_description TEXT,
  scenario_name TEXT,
  projection_start DATE,
  projection_end DATE,
  reporting_date DATE,
  created_at TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ
);

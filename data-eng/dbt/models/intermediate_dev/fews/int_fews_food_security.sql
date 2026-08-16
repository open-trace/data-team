{{ config(materialized='table') }}

-- intermediate_dev: FEWS food security domain.
-- Reads: stg_fews_food_security (population estimates + classifications,
--        already unioned with measure_type tag in staging).
-- Feeds: stg_silver_star_metrics spine â†’ fact_humanitarian â†’ fct_humanitarian (Gold).
--
-- Spine mapping:
--   domain_name       = 'humanitarian'
--   source_name       = source_natural_key
--   country_code      = country_code  (FEWS ISO-2 code)
--   country_name      = country       (FEWS country label)
--   admin_region      = admin_1       (first sub-national level; null for classifications)
--   period_year       = year          (extracted from projection_start in staging)
--   indicator_name    = measure_type  ('population' or 'classification') â€” the only
--                       metric descriptor FEWS provides at row level
--   unit_name         = null          (FEWS does not carry a unit column)
--   metric_value      = value         (population count or classification score)
--   ipc_phase_name    = phase_name    (IPC label e.g. 'Crisis', 'Emergency')
--   ipc_phase_numeric = phase_code cast to float64
--   shock_type_name   = null          (FEWS food-security tables do not classify shocks)
--   crop_name         = null          (not applicable)
--   all other spine columns = null

select
    'humanitarian'                          as domain_name,
    source_natural_key                      as source_name,
    country_code,
    country                                 as country_name,
    admin_1                                 as admin_region,
    cast(year as float64)                   as period_year,
    measure_type                            as indicator_name,
    cast(null as string)                    as unit_name,
    cast(value as float64)                  as metric_value,
    cast(null as string)                    as crop_name,
    cast(null as string)                    as season_name,
    cast(null as string)                    as practice_name,
    cast(null as string)                    as technology_name,
    phase_name                              as ipc_phase_name,
    safe_cast(phase_code as float64)        as ipc_phase_numeric,
    cast(null as string)                    as shock_type_name,
    cast(null as string)                    as policy_area_name,
    cast(null as string)                    as value_chain_stage_name,
    source_natural_key,
    loaded_at

from {{ ref('stg_fews_food_security') }}
where value is not null


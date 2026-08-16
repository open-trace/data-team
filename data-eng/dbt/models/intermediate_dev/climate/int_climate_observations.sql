{{ config(materialized='table') }}

-- intermediate_dev: climate observations domain.
-- Reads: stg_nasa_power (daily summary + hourly, already unioned in staging with
--        shared column names: par_solar_at_noon, shortwave_irradiance_at_noon,
--        uva_radiation_at_noon, uvb_radiation_at_noon).
-- Feeds: stg_silver_star_metrics spine â†’ fact_climate â†’ fct_climate (Gold).
--
-- NASA POWER data is sub-daily/daily radiation and solar, keyed by
-- country_code + admin_region (no calendar year â€” fetched_at carries the date).
-- The spine requires period_year; we extract it from fetched_at.
--
-- The four radiation measures are unpivoted into separate rows so each
-- produces its own indicator_name + metric_value pair in the spine.
-- This keeps grain consistent with the rest of the spine (one measure per row).
--
-- Spine mapping per row:
--   domain_name    = 'climate'
--   source_name    = source_natural_key   ('NASA_POWER_daily' or 'NASA_POWER_hourly')
--   country_code   = country_code
--   country_name   = country_name         (null for hourly rows â€” carried as-is)
--   admin_region   = admin_region
--   period_year    = extract(year from fetched_at)
--   indicator_name = radiation variable label (see UNION below)
--   unit_name      = 'W/m2' for irradiance / 'MJ/m2/day' for PAR (NASA standard)
--   metric_value   = the radiation measure value
--   all other spine columns = null

with src as (
    select
        source_natural_key,
        country_code,
        country_name,
        admin_region,
        cast(extract(year from fetched_at) as float64) as period_year,
        par_solar_at_noon,
        shortwave_irradiance_at_noon,
        uva_radiation_at_noon,
        uvb_radiation_at_noon,
        loaded_at
    from {{ ref('stg_nasa_power') }}
    where fetched_at is not null
      and (
            par_solar_at_noon           is not null
         or shortwave_irradiance_at_noon is not null
         or uva_radiation_at_noon        is not null
         or uvb_radiation_at_noon        is not null
      )
)

select
    'climate'              as domain_name,
    source_natural_key     as source_name,
    country_code,
    country_name,
    admin_region,
    period_year,
    'par_solar_at_noon'    as indicator_name,
    'MJ/m2/day'            as unit_name,
    par_solar_at_noon      as metric_value,
    cast(null as string)   as crop_name,
    cast(null as string)   as season_name,
    cast(null as string)   as practice_name,
    cast(null as string)   as technology_name,
    cast(null as string)   as ipc_phase_name,
    cast(null as float64)  as ipc_phase_numeric,
    cast(null as string)   as shock_type_name,
    cast(null as string)   as policy_area_name,
    cast(null as string)   as value_chain_stage_name,
    source_natural_key,
    loaded_at
from src where par_solar_at_noon is not null

union all

select
    'climate'                       as domain_name,
    source_natural_key              as source_name,
    country_code,
    country_name,
    admin_region,
    period_year,
    'shortwave_irradiance_at_noon'  as indicator_name,
    'W/m2'                          as unit_name,
    shortwave_irradiance_at_noon    as metric_value,
    cast(null as string)            as crop_name,
    cast(null as string)            as season_name,
    cast(null as string)            as practice_name,
    cast(null as string)            as technology_name,
    cast(null as string)            as ipc_phase_name,
    cast(null as float64)           as ipc_phase_numeric,
    cast(null as string)            as shock_type_name,
    cast(null as string)            as policy_area_name,
    cast(null as string)            as value_chain_stage_name,
    source_natural_key,
    loaded_at
from src where shortwave_irradiance_at_noon is not null

union all

select
    'climate'                as domain_name,
    source_natural_key       as source_name,
    country_code,
    country_name,
    admin_region,
    period_year,
    'uva_radiation_at_noon'  as indicator_name,
    'W/m2'                   as unit_name,
    uva_radiation_at_noon    as metric_value,
    cast(null as string)     as crop_name,
    cast(null as string)     as season_name,
    cast(null as string)     as practice_name,
    cast(null as string)     as technology_name,
    cast(null as string)     as ipc_phase_name,
    cast(null as float64)    as ipc_phase_numeric,
    cast(null as string)     as shock_type_name,
    cast(null as string)     as policy_area_name,
    cast(null as string)     as value_chain_stage_name,
    source_natural_key,
    loaded_at
from src where uva_radiation_at_noon is not null

union all

select
    'climate'                as domain_name,
    source_natural_key       as source_name,
    country_code,
    country_name,
    admin_region,
    period_year,
    'uvb_radiation_at_noon'  as indicator_name,
    'W/m2'                   as unit_name,
    uvb_radiation_at_noon    as metric_value,
    cast(null as string)     as crop_name,
    cast(null as string)     as season_name,
    cast(null as string)     as practice_name,
    cast(null as string)     as technology_name,
    cast(null as string)     as ipc_phase_name,
    cast(null as float64)    as ipc_phase_numeric,
    cast(null as string)     as shock_type_name,
    cast(null as string)     as policy_area_name,
    cast(null as string)     as value_chain_stage_name,
    source_natural_key,
    loaded_at
from src where uvb_radiation_at_noon is not null


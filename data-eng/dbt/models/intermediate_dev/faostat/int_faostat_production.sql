{{ config(materialized='table') }}

-- intermediate_dev: FAOSTAT production domain.
-- Reads: stg_faostat_production (Crops & Livestock + Production Indices +
--        Value of Agricultural Production â€” already unioned and renamed in staging).
-- Feeds: stg_silver_star_metrics spine â†’ fact_production â†’ fct_production (Gold).
--
-- Spine mapping:
--   domain_name     = 'production'
--   source_name     = source_natural_key (carried as-is for dim_source)
--   country_code    = area_code_m49      (M49 numeric code used as the country identifier)
--   country_name    = country_name       (FAOSTAT area â€” already renamed in staging)
--   admin_region    = null               (FAOSTAT production is national-level only)
--   period_year     = year               (cast to float64 to match spine contract)
--   indicator_name  = element            (e.g. 'Production', 'Area harvested', 'Yield')
--   unit_name       = unit
--   metric_value    = value
--   crop_name       = product_name       (FAOSTAT item â€” already renamed in staging)
--   all other spine columns = null       (not applicable for this domain)

select
    'production'                        as domain_name,
    source_natural_key                  as source_name,
    substr(cast(area_code_m49 as string), 2)      as country_code,
    country_name,
    cast(null as string)                as admin_region,
    cast(year as float64)               as period_year,
    element                             as indicator_name,
    unit                                as unit_name,
    value                               as metric_value,
    product_name                        as crop_name,
    cast(null as string)                as season_name,
    cast(null as string)                as practice_name,
    cast(null as string)                as technology_name,
    cast(null as string)                as ipc_phase_name,
    cast(null as float64)               as ipc_phase_numeric,
    cast(null as string)                as shock_type_name,
    cast(null as string)                as policy_area_name,
    cast(null as string)                as value_chain_stage_name,
    source_natural_key,
    loaded_at

from {{ ref('stg_faostat_production') }}
where value is not null


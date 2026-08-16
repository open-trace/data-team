{{ config(materialized='table') }}

-- intermediate_dev: FAOSTAT land inputs domain.
-- Reads: stg_faostat_land_inputs (10 raw_dev sources already unioned in staging:
--   fertilizers by nutrient/product, pesticides use, detailed trade matrix fertilizers,
--   land use, irrigation, manure applied/left on pasture,
--   temperature change on land, bioenergy, cropland nutrient balance, livestock patterns).
-- Feeds: stg_silver_star_metrics spine â†’ fact_land_use â†’ fct_land_use (Gold).
--
-- Spine mapping:
--   domain_name    = derived from source_natural_key:
--                    sources with 'Fertilizer' or 'fertilizer'  â†’ 'fertilizer'
--                    sources with 'Pesticide'  or 'Pesticides'  â†’ 'pesticide'
--                    all others (Land_Use, Irrigation, Manure,
--                    Temperature, Bioenergy, Nutrient_Balance,
--                    Livestock_Patterns)                        â†’ 'land_use'
--   source_name    = source_natural_key
--   country_code   = area_code_m49
--   country_name   = country_name
--   admin_region   = null  (FAOSTAT is national-level)
--   period_year    = year
--   indicator_name = element
--   unit_name      = unit
--   metric_value   = value
--   crop_name      = item  (fertilizer product / pesticide / land-use category)
--   all other spine columns = null

select
    case
        when source_natural_key like '%Fertilizer%'
          or source_natural_key like '%fertilizer%'  then 'fertilizer'
        when source_natural_key like '%Pesticide%'
          or source_natural_key like '%Pesticides%'  then 'pesticide'
        else                                              'land_use'
    end                                     as domain_name,
    source_natural_key                      as source_name,
    substr(cast(area_code_m49 as string), 2)  as country_code,
    country_name,
    cast(null as string)                    as admin_region,
    cast(year as float64)                   as period_year,
    element                                 as indicator_name,
    unit                                    as unit_name,
    cast(value as float64)                  as metric_value,
    -- For temperature rows item is null (months_code used instead); coalesce to months
    coalesce(item, months)                  as crop_name,
    cast(null as string)                    as season_name,
    cast(null as string)                    as practice_name,
    cast(null as string)                    as technology_name,
    cast(null as string)                    as ipc_phase_name,
    cast(null as float64)                   as ipc_phase_numeric,
    cast(null as string)                    as shock_type_name,
    cast(null as string)                    as policy_area_name,
    cast(null as string)                    as value_chain_stage_name,
    source_natural_key,
    loaded_at

from {{ ref('stg_faostat_land_inputs') }}
where value is not null




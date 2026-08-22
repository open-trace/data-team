{{ config(materialized='table') }}

-- Conformed silver star metrics view — unified spine for all staging_dev dimensions and facts.
-- Populated by the silver layer ingestion pipeline; this stub registers the schema
-- so staging_dev models compile while the silver pipeline is being built.

select
    cast(null as string)    as domain_name,
    cast(null as string)    as source_name,
    cast(null as string)    as source_key,
    cast(null as string)    as country_code,
    cast(null as string)    as country_name,
    cast(null as string)    as admin_region,
    cast(null as string)    as geography_key,
    cast(null as float64)   as period_year,
    cast(null as int64)     as period_key,
    cast(null as date)      as start_date,
    cast(null as string)    as indicator_name,
    cast(null as string)    as indicator_key,
    cast(null as string)    as unit_name,
    cast(null as string)    as unit_key,
    cast(null as float64)   as metric_value,
    cast(null as string)    as crop_name,
    cast(null as string)    as crop_key,
    cast(null as string)    as season_name,
    cast(null as string)    as season_key,
    cast(null as string)    as practice_name,
    cast(null as string)    as practice_key,
    cast(null as string)    as technology_name,
    cast(null as string)    as technology_key,
    cast(null as string)    as ipc_phase_name,
    cast(null as float64)   as ipc_phase_numeric,
    cast(null as string)    as ipc_phase_key,
    cast(null as string)    as shock_type_name,
    cast(null as string)    as shock_type_key,
    cast(null as string)    as policy_area_name,
    cast(null as string)    as policy_area_key,
    cast(null as string)    as value_chain_stage_name,
    cast(null as string)    as value_chain_stage_key,
    cast(null as float64)   as production_value,
    cast(null as float64)   as climate_value,
    cast(null as float64)   as humanitarian_value,
    cast(null as float64)   as market_access_value,
    cast(null as float64)   as land_use_value,
    cast(null as float64)   as technology_value,
    cast(null as float64)   as value_chain_value,
    cast(null as float64)   as enterprise_investment_value,
    cast(null as float64)   as nutrition_value,
    cast(null as float64)   as policy_value

where false

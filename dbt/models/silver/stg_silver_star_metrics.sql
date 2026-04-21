{{ config(materialized='view') }}

with climate as (
    select
        'africa_nasa_power_silver' as source_name,
        'climate' as domain_name,
        metric_name as indicator_name,
        country_code,
        country_name,
        admin_region,
        cast(null as int64) as period_year,
        cast(null as date) as period_date,
        'index' as unit_name,
        cast(null as string) as crop_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as season_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as technology_name,
        cast(null as string) as practice_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        metric_value
    from (
        select country_code, country_name, admin_region, 'par_solar_at_noon' as metric_name, cast(par_solar_at_noon as float64) as metric_value
        from {{ source('silver', 'africa_nasa_power_silver') }}
        union all
        select country_code, country_name, admin_region, 'shortwave_irradiance_at_noon' as metric_name, cast(shortwave_irradiance_at_noon as float64) as metric_value
        from {{ source('silver', 'africa_nasa_power_silver') }}
        union all
        select country_code, country_name, admin_region, 'uva_radiation_at_noon' as metric_name, cast(uva_radiation_at_noon as float64) as metric_value
        from {{ source('silver', 'africa_nasa_power_silver') }}
        union all
        select country_code, country_name, admin_region, 'uvb_radiation_at_noon' as metric_name, cast(uvb_radiation_at_noon as float64) as metric_value
        from {{ source('silver', 'africa_nasa_power_silver') }}
    )
),
land_use as (
    select
        'fao_rl_landuse' as source_name,
        'land_use' as domain_name,
        coalesce(element, item) as indicator_name,
        country_code,
        country_name,
        cast(null as string) as admin_region,
        cast(year as int64) as period_year,
        cast(null as date) as period_date,
        unit as unit_name,
        item as crop_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as season_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as technology_name,
        cast(null as string) as practice_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(value as float64) as metric_value
    from {{ source('silver', 'fao_rl_landuse') }}
),
enterprise_investment as (
    select
        'fao_qi' as source_name,
        'enterprise_investment' as domain_name,
        coalesce(element, item) as indicator_name,
        country_code,
        country_name,
        cast(null as string) as admin_region,
        cast(year as int64) as period_year,
        cast(null as date) as period_date,
        unit as unit_name,
        item as crop_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as season_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as technology_name,
        cast(null as string) as practice_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(value as float64) as metric_value
    from {{ source('silver', 'fao_qi') }}
),
technology as (
    select
        'fao_ti_trade_indices' as source_name,
        'technology' as domain_name,
        coalesce(element, item) as indicator_name,
        country_code,
        country_name,
        cast(null as string) as admin_region,
        cast(year as int64) as period_year,
        cast(null as date) as period_date,
        unit as unit_name,
        item as crop_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as season_name,
        cast(null as string) as policy_area_name,
        item as technology_name,
        cast(null as string) as practice_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(value as float64) as metric_value
    from {{ source('silver', 'fao_ti_trade_indices') }}
),
market_access as (
    select
        'fao_qv' as source_name,
        'market_access' as domain_name,
        coalesce(element, item) as indicator_name,
        country_code,
        country_name,
        cast(null as string) as admin_region,
        cast(year as int64) as period_year,
        cast(null as date) as period_date,
        unit as unit_name,
        item as crop_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as season_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as technology_name,
        cast(null as string) as practice_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(value as float64) as metric_value
    from {{ source('silver', 'fao_qv') }}
),
production as (
    select
        'yield_raw_data_silver' as source_name,
        'production' as domain_name,
        metric_name as indicator_name,
        country_code,
        country as country_name,
        admin_1 as admin_region,
        harvest_year as period_year,
        cast(null as date) as period_date,
        metric_unit as unit_name,
        product as crop_name,
        cast(null as string) as value_chain_stage_name,
        season_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as technology_name,
        crop_production_system as practice_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        metric_value
    from (
        select
            country_code,
            country,
            admin_1,
            harvest_year,
            season_name,
            crop_production_system,
            product,
            'area' as metric_name,
            cast(area as float64) as metric_value,
            'ha' as metric_unit
        from {{ source('silver', 'yield_raw_data_silver') }}
        union all
        select
            country_code,
            country,
            admin_1,
            harvest_year,
            season_name,
            crop_production_system,
            product,
            'production' as metric_name,
            cast(production as float64) as metric_value,
            'production_unit' as metric_unit
        from {{ source('silver', 'yield_raw_data_silver') }}
        union all
        select
            country_code,
            country,
            admin_1,
            harvest_year,
            season_name,
            crop_production_system,
            product,
            'yield' as metric_name,
            cast(yield as float64) as metric_value,
            'yield_unit' as metric_unit
        from {{ source('silver', 'yield_raw_data_silver') }}
    )
),
policy as (
    select
        'fao_rp_pesticides' as source_name,
        'policy' as domain_name,
        coalesce(element, item) as indicator_name,
        country_code,
        country_name,
        cast(null as string) as admin_region,
        cast(year as int64) as period_year,
        cast(null as date) as period_date,
        unit as unit_name,
        item as crop_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as season_name,
        element as policy_area_name,
        cast(null as string) as technology_name,
        cast(null as string) as practice_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(value as float64) as metric_value
    from {{ source('silver', 'fao_rp_pesticides') }}
),
nutrition as (
    select
        'fao_rhn' as source_name,
        'nutrition' as domain_name,
        coalesce(element, item) as indicator_name,
        country_code,
        country_name,
        cast(null as string) as admin_region,
        cast(year as int64) as period_year,
        cast(null as date) as period_date,
        unit as unit_name,
        item as crop_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as season_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as technology_name,
        cast(null as string) as practice_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(value as float64) as metric_value
    from {{ source('silver', 'fao_rhn') }}
),
value_chain as (
    select
        'fao_fbs' as source_name,
        'value_chain' as domain_name,
        coalesce(element, item) as indicator_name,
        country_code,
        country_name,
        cast(null as string) as admin_region,
        cast(year as int64) as period_year,
        cast(null as date) as period_date,
        unit as unit_name,
        item as crop_name,
        element as value_chain_stage_name,
        cast(null as string) as season_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as technology_name,
        cast(null as string) as practice_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(value as float64) as metric_value
    from {{ source('silver', 'fao_fbs') }}
),
humanitarian as (
    select
        'fews_net_food_security_master_silver' as source_name,
        'humanitarian' as domain_name,
        'ipc_phase_value' as indicator_name,
        country_code,
        country as country_name,
        geographic_unit_name as admin_region,
        extract(year from reporting_date) as period_year,
        reporting_date as period_date,
        'ipc_index' as unit_name,
        cast(null as string) as crop_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as season_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as technology_name,
        unit_type as practice_name,
        scenario_name as shock_type_name,
        ipc_description as ipc_phase_name,
        cast(ipc_phase_value as float64) as ipc_phase_numeric,
        cast(ipc_phase_value as float64) as metric_value
    from {{ source('silver', 'fews_net_food_security_master_silver') }}
)

select * from climate
union all
select * from land_use
union all
select * from enterprise_investment
union all
select * from technology
union all
select * from market_access
union all
select * from production
union all
select * from policy
union all
select * from nutrition
union all
select * from value_chain
union all
select * from humanitarian

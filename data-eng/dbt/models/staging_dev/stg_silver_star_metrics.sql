{{ config(materialized='view') }}
-- stg_silver_star_metrics: unified silver staging layer
-- All column names verified against sources.yml

with fao_food_security as (
    select
        'nutrition' as domain_name,
        cast(area_code_m49 as string) as country_code,
        area as country_name,
        item as indicator_name,
        'FAO Food Security' as source_name,
        unit as unit_name,
        cast(null as string) as admin_region,
        cast(null as string) as crop_name,
        cast(null as string) as season_name,
        cast(null as string) as practice_name,
        cast(null as string) as technology_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(2022 as int64) as period_year,
        cast(null as float64) as metric_value
    from {{ source('landing', 'FAOstat_africa_Food_Security_and_Nutrition_Suite_of_Food_Security_Indicators') }}
    where area_code_m49 is not null

),

fao_production as (
    select
        'production' as domain_name,
        cast(area_code_m49 as string) as country_code,
        area as country_name,
        element as indicator_name,
        'FAO Production' as source_name,
        unit as unit_name,
        cast(null as string) as admin_region,
        item as crop_name,
        cast(null as string) as season_name,
        cast(null as string) as practice_name,
        cast(null as string) as technology_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(2022 as int64) as period_year,
        cast(null as float64) as metric_value
    from {{ source('landing', 'FAOstat_africa_production_Crops_and_livestock') }}
    where area_code_m49 is not null

),

fews_humanitarian as (
    select
        'humanitarian' as domain_name,
        cast(country_code as string) as country_code,
        country as country_name,
        geographic_unit_name as admin_region,
        classification_scale as indicator_name,
        'FEWS NET Classifications' as source_name,
        cast(null as string) as unit_name,
        cast(null as string) as crop_name,
        cast(null as string) as season_name,
        cast(null as string) as practice_name,
        cast(null as string) as technology_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as policy_area_name,
        scenario_name as shock_type_name,
        classification_scale as ipc_phase_name,
        cast(value as float64) as ipc_phase_numeric,
        cast(2022 as int64) as period_year,
        cast(value as float64) as metric_value
    from {{ source('landing', 'FEWS_NET_food_security_classifications_time_series_data') }}
    where country_code is not null

),

fews_market as (
    select
        'market_access' as domain_name,
        cast(country_code as string) as country_code,
        country as country_name,
        cast(null as string) as admin_region,
        product as indicator_name,
        'FEWS NET Market Prices' as source_name,
        unit as unit_name,
        product as crop_name,
        cast(null as string) as season_name,
        cast(null as string) as practice_name,
        cast(null as string) as technology_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(2022 as int64) as period_year,
        cast(value as float64) as metric_value
    from {{ source('landing', 'FEWS_NET_market_Prices_time_series_data') }}
    where country_code is not null

),

wfp_prices as (
    select
        'market_access' as domain_name,
        cast(adm0_name as string) as country_code,
        adm0_name as country_name,
        cast(null as string) as admin_region,
        cm_name as indicator_name,
        'WFP VAMPIRE' as source_name,
        cur_name as unit_name,
        cm_name as crop_name,
        cast(null as string) as season_name,
        cast(null as string) as practice_name,
        cast(null as string) as technology_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(mp_year as int64) as period_year,
        cast(mp_price as float64) as metric_value
    from {{ source('landing', 'WFP_VAMPIRE_Tool_global_food_prices') }}
    where adm0_name is not null

),

world_gdp as (
    select
        'enterprise_investment' as domain_name,
        cast(alpha_3_code as string) as country_code,
        cast(null as string) as country_name,
        cast(null as string) as admin_region,
        'GDP per capita PPP' as indicator_name,
        'World Bank' as source_name,
        cast(null as string) as unit_name,
        cast(null as string) as crop_name,
        cast(null as string) as season_name,
        cast(null as string) as practice_name,
        cast(null as string) as technology_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(year as int64) as period_year,
        cast(gdp_per_capita as float64) as metric_value
    from {{ source('landing', 'world_gdp_ppp') }}
    where alpha_3_code is not null

),

world_hdi as (
    select
        'policy' as domain_name,
        cast(code as string) as country_code,
        cast(null as string) as country_name,
        cast(null as string) as admin_region,
        'Human Development Index' as indicator_name,
        'UNDP HDI' as source_name,
        cast(null as string) as unit_name,
        cast(null as string) as crop_name,
        cast(null as string) as season_name,
        cast(null as string) as practice_name,
        cast(null as string) as technology_name,
        cast(null as string) as value_chain_stage_name,
        cast(null as string) as policy_area_name,
        cast(null as string) as shock_type_name,
        cast(null as string) as ipc_phase_name,
        cast(null as float64) as ipc_phase_numeric,
        cast(year as int64) as period_year,
        cast(index as float64) as metric_value
    from {{ source('landing', 'world_Human_development_index') }}
    where code is not null
)


select * from fao_food_security
union all select * from fao_production
union all select * from fews_humanitarian
union all select * from fews_market
union all select * from wfp_prices
union all select * from world_gdp
union all select * from world_hdi

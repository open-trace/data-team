{{ config(materialized='table') }}

-- Conformed silver star metrics spine — unified input for all staging_dev dims + facts.
-- Unions all intermediate_dev domain models into the single 40-column contract that
-- every dim_* and fact_* model in this layer reads via ref('stg_silver_star_metrics').
--
-- Intermediate models that feed this spine:
--   int_faostat_production   → domain_name = 'production'
--   int_faostat_land_inputs  → domain_name = 'land_use' | 'fertilizer' | 'pesticide'
--   int_fews_food_security   → domain_name = 'humanitarian'
--   int_fews_market_prices   → domain_name = 'market_access'
--   int_climate_observations → domain_name = 'climate'
--
-- Derived/computed columns (surrogate keys and domain-value aliases) are added
-- here so every downstream dim_* and fact_* model receives a fully-keyed row
-- without each having to repeat the md5 expressions.
--
-- Columns that intermediate models do not populate are cast to null with the
-- correct type so the UNION ALL schema stays fixed.

with combined as (

    select * from {{ ref('int_faostat_production') }}
    union all
    select * from {{ ref('int_faostat_land_inputs') }}
    union all
    select * from {{ ref('int_fews_food_security') }}
    union all
    select * from {{ ref('int_fews_market_prices') }}
    union all
    select * from {{ ref('int_climate_observations') }}

)

select
    -- ── Domain / source ──────────────────────────────────────────────────────
    domain_name,
    source_name,
    to_hex(md5(coalesce(source_name, '')))
                                                as source_key,

    -- ── Geography ────────────────────────────────────────────────────────────
    country_code,
    country_name,
    admin_region,
    to_hex(md5(concat(
        coalesce(country_code,  ''), '|',
        coalesce(country_name,  ''), '|',
        coalesce(admin_region,  '')
    )))                                         as geography_key,

    -- ── Time ─────────────────────────────────────────────────────────────────
    period_year,
    cast(period_year as int64)                  as period_key,
    date(cast(period_year as int64), 1, 1)      as start_date,

    -- ── Indicator / unit ─────────────────────────────────────────────────────
    indicator_name,
    to_hex(md5(coalesce(indicator_name, '')))   as indicator_key,
    unit_name,
    to_hex(md5(coalesce(unit_name, '')))        as unit_key,

    -- ── Measure ──────────────────────────────────────────────────────────────
    metric_value,

    -- ── Product / crop ───────────────────────────────────────────────────────
    crop_name,
    to_hex(md5(coalesce(crop_name, '')))        as crop_key,

    -- ── Season / practice / technology (not populated by current domains) ────
    season_name,
    to_hex(md5(coalesce(season_name, '')))      as season_key,
    practice_name,
    to_hex(md5(coalesce(practice_name, '')))    as practice_key,
    technology_name,
    to_hex(md5(coalesce(technology_name, '')))  as technology_key,

    -- ── IPC / humanitarian ───────────────────────────────────────────────────
    ipc_phase_name,
    ipc_phase_numeric,
    to_hex(md5(concat(
        coalesce(ipc_phase_name, ''), '|',
        coalesce(cast(ipc_phase_numeric as string), '')
    )))                                         as ipc_phase_key,
    shock_type_name,
    to_hex(md5(coalesce(shock_type_name, '')))  as shock_type_key,

    -- ── Policy / value-chain (not populated by current domains) ─────────────
    policy_area_name,
    to_hex(md5(coalesce(policy_area_name, ''))) as policy_area_key,
    value_chain_stage_name,
    to_hex(md5(coalesce(value_chain_stage_name, '')))
                                                as value_chain_stage_key,

    -- ── Domain-aliased measure columns (used by individual fact_* models) ────
    case when domain_name = 'production'
         then metric_value end                  as production_value,
    case when domain_name = 'climate'
         then metric_value end                  as climate_value,
    case when domain_name = 'humanitarian'
         then metric_value end                  as humanitarian_value,
    case when domain_name = 'market_access'
         then metric_value end                  as market_access_value,
    case when domain_name = 'land_use'
         then metric_value end                  as land_use_value,
    case when domain_name = 'technology'
         then metric_value end                  as technology_value,
    case when domain_name = 'value_chain'
         then metric_value end                  as value_chain_value,
    case when domain_name = 'enterprise_investment'
         then metric_value end                  as enterprise_investment_value,
    case when domain_name = 'nutrition'
         then metric_value end                  as nutrition_value,
    case when domain_name = 'policy'
         then metric_value end                  as policy_value

from combined
where metric_value is not null
  and country_code  is not null

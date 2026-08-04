{{ config(materialized='ephemeral') }}

-- int_enriched_metrics
-- Intermediate layer: joins the unified silver metrics with country dimension
-- for downstream mart use. Ephemeral = no BQ table created, inlined as CTE.
-- Extend this model to join additional dims or apply business logic.

select
    m.domain_name,
    m.country_code,
    coalesce(m.country_name, c.country_name)  as country_name,
    m.admin_region,
    m.indicator_name,
    m.source_name,
    m.unit_name,
    m.crop_name,
    m.season_name,
    m.practice_name,
    m.technology_name,
    m.value_chain_stage_name,
    m.policy_area_name,
    m.shock_type_name,
    m.ipc_phase_name,
    m.ipc_phase_numeric,
    m.period_year,
    m.metric_value
from {{ ref('stg_silver_star_metrics') }} m
left join {{ ref('dim_country') }} c
    on m.country_code = c.country_code


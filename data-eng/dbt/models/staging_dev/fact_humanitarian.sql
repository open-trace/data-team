{{ config(materialized='table') }}

select
    to_hex(md5(concat(
        coalesce(source_name, ''), '|',
        coalesce(country_code, ''), '|',
        coalesce(cast(period_year as string), ''), '|',
        coalesce(ipc_phase_name, ''), '|',
        coalesce(shock_type_name, '')
    ))) as fact_humanitarian_key,
    to_hex(md5(concat(coalesce(country_code, ''), '|', coalesce(country_name, '')))) as country_key,
    cast(period_year as int64) as period_key,
    to_hex(md5(concat(coalesce(country_code, ''), '|', coalesce(country_name, ''), '|', coalesce(admin_region, '')))) as geography_key,
    to_hex(md5(coalesce(indicator_name, ''))) as indicator_key,
    to_hex(md5(coalesce(source_name, ''))) as source_key,
    to_hex(md5(coalesce(unit_name, ''))) as unit_key,
    to_hex(md5(coalesce(shock_type_name, ''))) as shock_type_key,
    to_hex(md5(concat(coalesce(ipc_phase_name, ''), '|', coalesce(cast(ipc_phase_numeric as string), '')))) as ipc_phase_key,
    metric_value as humanitarian_value
from {{ ref('stg_silver_star_metrics') }}
where domain_name = 'humanitarian'

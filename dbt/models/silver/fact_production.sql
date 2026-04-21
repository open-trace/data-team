{{ config(materialized='table') }}

select
    to_hex(md5(concat(
        coalesce(source_name, ''), '|',
        coalesce(indicator_name, ''), '|',
        coalesce(country_code, ''), '|',
        coalesce(cast(period_year as string), ''), '|',
        coalesce(crop_name, ''), '|',
        coalesce(season_name, '')
    ))) as fact_production_key,
    to_hex(md5(concat(coalesce(country_code, ''), '|', coalesce(country_name, '')))) as country_key,
    cast(period_year as int64) as period_key,
    to_hex(md5(concat(coalesce(country_code, ''), '|', coalesce(country_name, ''), '|', coalesce(admin_region, '')))) as geography_key,
    to_hex(md5(coalesce(indicator_name, ''))) as indicator_key,
    to_hex(md5(coalesce(source_name, ''))) as source_key,
    to_hex(md5(coalesce(unit_name, ''))) as unit_key,
    to_hex(md5(coalesce(crop_name, ''))) as crop_key,
    to_hex(md5(coalesce(season_name, ''))) as season_key,
    to_hex(md5(coalesce(practice_name, ''))) as practice_key,
    metric_value as production_value
from {{ ref('stg_silver_star_metrics') }}
where domain_name = 'production'

{{ config(materialized='table') }}

select distinct
    to_hex(md5(concat(coalesce(country_code, ''), '|', coalesce(country_name, ''), '|', coalesce(admin_region, '')))) as geography_key,
    country_code,
    country_name,
    admin_region
from {{ ref('stg_silver_star_metrics') }}
where country_code is not null or country_name is not null or admin_region is not null

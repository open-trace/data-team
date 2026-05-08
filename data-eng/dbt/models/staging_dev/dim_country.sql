{{ config(materialized='table') }}

select distinct
    to_hex(md5(concat(coalesce(country_code, ''), '|', coalesce(country_name, '')))) as country_key,
    country_code,
    country_name
from {{ ref('stg_silver_star_metrics') }}
where country_code is not null or country_name is not null

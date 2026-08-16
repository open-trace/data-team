{{ config(materialized='table') }}

select distinct
    to_hex(md5(coalesce(indicator_name, ''))) as indicator_key,
    indicator_name
from {{ ref('stg_silver_star_metrics') }}
where indicator_name is not null

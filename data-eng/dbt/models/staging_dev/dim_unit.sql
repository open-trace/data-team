{{ config(materialized='table') }}

select distinct
    to_hex(md5(coalesce(unit_name, ''))) as unit_key,
    unit_name
from {{ ref('stg_silver_star_metrics') }}
where unit_name is not null

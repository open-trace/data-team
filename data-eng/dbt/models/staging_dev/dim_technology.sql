{{ config(materialized='table') }}

select distinct
    to_hex(md5(coalesce(technology_name, ''))) as technology_key,
    technology_name
from {{ ref('stg_silver_star_metrics') }}
where technology_name is not null

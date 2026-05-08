{{ config(materialized='table') }}

select distinct
    to_hex(md5(coalesce(source_name, ''))) as source_key,
    source_name
from {{ ref('stg_silver_star_metrics') }}
where source_name is not null

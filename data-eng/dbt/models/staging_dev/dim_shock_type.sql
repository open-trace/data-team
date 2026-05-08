{{ config(materialized='table') }}

select distinct
    to_hex(md5(coalesce(shock_type_name, ''))) as shock_type_key,
    shock_type_name
from {{ ref('stg_silver_star_metrics') }}
where shock_type_name is not null

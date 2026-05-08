{{ config(materialized='table') }}

select distinct
    to_hex(md5(coalesce(policy_area_name, ''))) as policy_area_key,
    policy_area_name
from {{ ref('stg_silver_star_metrics') }}
where policy_area_name is not null

{{ config(materialized='table') }}

select distinct
    to_hex(md5(coalesce(value_chain_stage_name, ''))) as value_chain_stage_key,
    value_chain_stage_name
from {{ ref('stg_silver_star_metrics') }}
where value_chain_stage_name is not null

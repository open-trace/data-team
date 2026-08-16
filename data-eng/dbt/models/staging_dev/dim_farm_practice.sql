{{ config(materialized='table') }}

select distinct
    to_hex(md5(coalesce(practice_name, ''))) as practice_key,
    practice_name
from {{ ref('stg_silver_star_metrics') }}
where practice_name is not null

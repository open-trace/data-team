{{ config(materialized='table') }}

select distinct
    to_hex(md5(coalesce(season_name, ''))) as season_key,
    season_name
from {{ ref('stg_silver_star_metrics') }}
where season_name is not null

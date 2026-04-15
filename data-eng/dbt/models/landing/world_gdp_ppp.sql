{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'world_gdp_ppp') }}

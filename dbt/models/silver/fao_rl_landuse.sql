{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('silver', 'fao_rl_landuse') }}

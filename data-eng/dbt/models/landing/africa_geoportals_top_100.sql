{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'africa_geoportals_top_100') }}

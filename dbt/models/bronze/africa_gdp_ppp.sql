{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'africa_gdp_ppp') }}

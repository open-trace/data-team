{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('silver', 'africa_nasa_power_silver') }}

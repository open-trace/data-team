{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'africa_nasa_power_raw') }}

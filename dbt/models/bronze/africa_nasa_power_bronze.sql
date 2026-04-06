{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'africa_nasa_power_bronze') }}

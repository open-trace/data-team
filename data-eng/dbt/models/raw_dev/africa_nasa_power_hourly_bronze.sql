{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'africa_nasa_power_hourly_bronze') }}

{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('staging_dev', 'africa_nasa_power_silver') }}

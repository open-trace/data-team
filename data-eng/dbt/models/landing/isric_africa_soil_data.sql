{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'isric_africa_soil_data') }}

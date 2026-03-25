{{ config(materialized='view') }}

select
    *
from {{ source('landing', 'isric_africa_soil_data') }}

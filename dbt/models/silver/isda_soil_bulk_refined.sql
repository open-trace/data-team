{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('silver', 'isda_soil_bulk_refined') }}

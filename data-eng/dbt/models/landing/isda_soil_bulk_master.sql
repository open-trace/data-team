{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'isda_soil_bulk_master') }}

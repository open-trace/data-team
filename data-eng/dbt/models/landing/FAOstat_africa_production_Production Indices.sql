{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'FAOstat_africa_production_Production Indices') }}

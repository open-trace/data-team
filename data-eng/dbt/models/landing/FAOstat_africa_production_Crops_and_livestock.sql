{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'FAOstat_africa_production_Crops_and_livestock') }}

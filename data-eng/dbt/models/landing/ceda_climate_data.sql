{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'ceda_climate_data') }}

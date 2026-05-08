{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'isda_africa_carbon_bulk') }}

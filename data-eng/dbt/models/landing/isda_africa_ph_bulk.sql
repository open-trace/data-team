{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'isda_africa_ph_bulk') }}

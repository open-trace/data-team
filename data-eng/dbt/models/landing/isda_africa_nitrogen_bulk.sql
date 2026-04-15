{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'isda_africa_nitrogen_bulk') }}

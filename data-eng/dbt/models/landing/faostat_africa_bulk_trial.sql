{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'faostat_africa_bulk_trial') }}

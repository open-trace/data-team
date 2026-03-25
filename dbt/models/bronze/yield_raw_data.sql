{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'yield_raw_data') }}

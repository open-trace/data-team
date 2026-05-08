{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'yield_raw_data') }}

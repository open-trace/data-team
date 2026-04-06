{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('silver', 'yield_raw_data_silver') }}

{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('staging_dev', 'yield_raw_data_silver') }}

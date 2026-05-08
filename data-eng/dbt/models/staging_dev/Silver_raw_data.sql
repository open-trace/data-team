{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('staging_dev', 'Silver_raw_data') }}

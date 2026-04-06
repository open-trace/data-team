{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('silver', 'Silver_raw_data') }}

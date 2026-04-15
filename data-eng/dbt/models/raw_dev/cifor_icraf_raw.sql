{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'cifor_icraf_raw') }}

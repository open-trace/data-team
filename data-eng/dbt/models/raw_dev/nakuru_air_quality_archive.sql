{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'nakuru_air_quality_archive') }}

{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'fao_land_use_bronze') }}

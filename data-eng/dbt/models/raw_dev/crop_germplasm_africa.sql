{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'crop_germplasm_africa') }}

{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'africa_crop_production_full_melted') }}

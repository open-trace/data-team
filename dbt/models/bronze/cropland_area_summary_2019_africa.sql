{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'cropland_area_summary_2019_africa') }}

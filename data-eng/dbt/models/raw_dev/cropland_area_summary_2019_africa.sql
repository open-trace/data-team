{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'cropland_area_summary_2019_africa') }}

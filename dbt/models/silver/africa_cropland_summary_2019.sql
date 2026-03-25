{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('silver', 'africa_cropland_summary_2019') }}

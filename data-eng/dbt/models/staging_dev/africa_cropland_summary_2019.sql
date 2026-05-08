{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('staging_dev', 'africa_cropland_summary_2019') }}

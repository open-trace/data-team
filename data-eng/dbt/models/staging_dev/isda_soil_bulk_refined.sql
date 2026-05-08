{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('staging_dev', 'isda_soil_bulk_refined') }}

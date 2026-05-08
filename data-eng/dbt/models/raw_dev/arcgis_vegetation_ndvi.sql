{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'arcgis_vegetation_ndvi') }}

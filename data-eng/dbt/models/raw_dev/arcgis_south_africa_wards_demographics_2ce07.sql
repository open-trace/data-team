{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'arcgis_south_africa_wards_demographics_2ce07') }}

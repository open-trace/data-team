{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'arcgis_layer_rice_germplasm_in_africa_3d2a9') }}

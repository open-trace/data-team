{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('silver', 'fao_tcl_crop_and_pest') }}

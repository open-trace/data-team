{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('staging_dev', 'fao_tcl_crop_and_pest') }}

{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'isda_soil_bulk_master_bronze') }}

{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'isda_soil_bulk_master_bronze') }}

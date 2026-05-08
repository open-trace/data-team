{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'ifpri_africa_bronze') }}

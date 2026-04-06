{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'ifpri_africa_bronze') }}

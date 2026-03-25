{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'fao_ti') }}

{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'africa_Human_development_index') }}

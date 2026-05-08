{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'world_Human_development_index') }}

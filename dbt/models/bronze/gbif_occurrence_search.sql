{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'gbif_occurrence_search') }}

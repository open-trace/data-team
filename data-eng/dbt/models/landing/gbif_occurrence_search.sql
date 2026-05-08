{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'gbif_occurrence_search') }}

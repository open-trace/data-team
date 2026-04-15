{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'gbif_occurrence_search') }}

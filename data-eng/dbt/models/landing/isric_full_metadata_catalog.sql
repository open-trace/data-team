{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'isric_full_metadata_catalog') }}

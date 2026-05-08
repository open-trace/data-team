{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'gbif_biodiversity_occurrence') }}

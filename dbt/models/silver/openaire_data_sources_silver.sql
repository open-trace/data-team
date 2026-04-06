{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('silver', 'openaire_data_sources_silver') }}

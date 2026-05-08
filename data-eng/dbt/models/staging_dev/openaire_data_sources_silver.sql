{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('staging_dev', 'openaire_data_sources_silver') }}

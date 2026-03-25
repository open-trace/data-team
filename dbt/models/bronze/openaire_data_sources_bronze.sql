{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'openaire_data_sources_bronze') }}

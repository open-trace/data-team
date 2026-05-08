{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'openaire_agriculture_and_environment_Research_publications_Data_sources_bronze') }}

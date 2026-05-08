{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'climatewatch_emission_pathways') }}

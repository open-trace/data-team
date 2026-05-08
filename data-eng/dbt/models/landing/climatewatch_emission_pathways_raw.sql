{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'climatewatch_emission_pathways_raw') }}

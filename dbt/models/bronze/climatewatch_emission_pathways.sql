{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'climatewatch_emission_pathways') }}

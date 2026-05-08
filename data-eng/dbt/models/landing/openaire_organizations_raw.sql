{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'openaire_organizations_raw') }}

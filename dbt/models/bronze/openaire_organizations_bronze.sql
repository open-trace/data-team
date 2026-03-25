{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'openaire_organizations_bronze') }}

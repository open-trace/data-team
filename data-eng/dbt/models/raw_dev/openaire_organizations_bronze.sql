{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'openaire_organizations_bronze') }}

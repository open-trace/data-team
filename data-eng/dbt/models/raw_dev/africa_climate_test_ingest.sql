{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'africa_climate_test_ingest') }}

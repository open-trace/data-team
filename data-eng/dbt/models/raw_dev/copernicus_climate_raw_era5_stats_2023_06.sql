{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'copernicus_climate_raw_era5_stats_2023_06') }}

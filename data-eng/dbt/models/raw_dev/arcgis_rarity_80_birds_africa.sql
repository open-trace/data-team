{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'arcgis_rarity_80_birds_africa') }}

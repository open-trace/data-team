{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'fao_fertilizers_nutrient_bronze') }}

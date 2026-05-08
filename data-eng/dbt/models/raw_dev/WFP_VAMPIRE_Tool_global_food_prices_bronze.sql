{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'WFP_VAMPIRE_Tool_global_food_prices_bronze') }}

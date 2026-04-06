{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'WFP_VAMPIRE_Tool_global_food_prices') }}

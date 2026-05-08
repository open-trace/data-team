{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'WFP_VAMPIRE_Tool_global_food_prices') }}

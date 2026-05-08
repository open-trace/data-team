{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'ilri_crp_household_food_security_v1') }}

{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'fews_net_food_security_master') }}

{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'fews_net_food_security_master') }}

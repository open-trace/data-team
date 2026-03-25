{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('silver', 'fews_net_food_security_master_alldata_silver') }}

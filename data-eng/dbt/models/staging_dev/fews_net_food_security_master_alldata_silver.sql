{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('staging_dev', 'fews_net_food_security_master_alldata_silver') }}

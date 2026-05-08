{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'FEWS_NET_food_security_classifications_data_series') }}

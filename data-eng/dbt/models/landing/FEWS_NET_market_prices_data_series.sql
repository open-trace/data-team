{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'FEWS_NET_market_prices_data_series') }}

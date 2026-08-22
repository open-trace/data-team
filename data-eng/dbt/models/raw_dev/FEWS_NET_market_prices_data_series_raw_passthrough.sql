{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'FEWS_NET_market_prices_data_series') }}

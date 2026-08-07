{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'FEWS_NET_market_Prices_time_series_data') }}

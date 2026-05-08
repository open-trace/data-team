{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'FEWS_NET_market_Prices_time_series_data') }}

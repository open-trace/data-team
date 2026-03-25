{{ config(materialized='view') }}

select
    *
from {{ source('landing', 'FEWS_NET_cross_border_trade_data_series') }}

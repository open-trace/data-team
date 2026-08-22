{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'FEWS_NET_cross_border_trade_data_series') }}

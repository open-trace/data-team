{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'FEWS_NET_cross_border_Trade_time_series_data') }}

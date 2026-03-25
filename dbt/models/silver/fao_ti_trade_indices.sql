{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('silver', 'fao_ti_trade_indices') }}

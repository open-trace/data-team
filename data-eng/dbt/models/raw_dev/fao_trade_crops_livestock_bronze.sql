{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'fao_trade_crops_livestock_bronze') }}

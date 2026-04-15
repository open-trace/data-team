{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('staging_dev', 'fao_ti_trade_indices') }}

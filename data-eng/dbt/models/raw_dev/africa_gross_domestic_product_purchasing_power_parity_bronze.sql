{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'africa_gross_domestic_product_purchasing_power_parity_bronze') }}

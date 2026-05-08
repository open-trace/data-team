{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'openaire_product_links_raw') }}

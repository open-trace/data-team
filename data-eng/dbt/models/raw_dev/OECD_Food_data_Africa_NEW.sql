{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'OECD_Food_data_Africa_NEW') }}

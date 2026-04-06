{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('bronze', 'OECD_Food_data_Africa_NEW') }}

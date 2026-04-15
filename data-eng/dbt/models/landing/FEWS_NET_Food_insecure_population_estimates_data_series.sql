{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'FEWS_NET_Food_insecure_population_estimates_data_series') }}

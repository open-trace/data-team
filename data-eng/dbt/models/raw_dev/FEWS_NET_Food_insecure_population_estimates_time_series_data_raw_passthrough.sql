{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'FEWS_NET_Food_insecure_population_estimates_time_series_data') }}

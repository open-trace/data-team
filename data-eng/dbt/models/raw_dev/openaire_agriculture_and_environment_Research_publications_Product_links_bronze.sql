{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('raw_dev', 'openaire_agriculture_and_environment_Research_publications_Product_links_bronze') }}

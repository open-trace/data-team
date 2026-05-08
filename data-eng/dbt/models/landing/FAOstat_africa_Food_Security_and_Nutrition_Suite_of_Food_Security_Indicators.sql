{{ config(materialized='view', enabled=false) }}

select
    *
from {{ source('landing', 'FAOstat_africa_Food_Security_and_Nutrition_Suite_of_Food_Security_Indicators') }}

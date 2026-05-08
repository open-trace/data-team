{{ config(materialized='table') }}

select distinct
    to_hex(md5(coalesce(crop_name, ''))) as crop_key,
    crop_name
from {{ ref('stg_silver_star_metrics') }}
where crop_name is not null

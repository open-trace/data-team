{{ config(materialized='table') }}

select distinct
    to_hex(md5(concat(coalesce(ipc_phase_name, ''), '|', coalesce(cast(ipc_phase_numeric as string), '')))) as ipc_phase_key,
    ipc_phase_name,
    ipc_phase_numeric
from {{ ref('stg_silver_star_metrics') }}
where ipc_phase_name is not null or ipc_phase_numeric is not null

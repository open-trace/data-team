-- Example bronze staging model. Replace with real source tables and logic.
-- Reads from landing/bronze; runs when: dbt run --target bronze
-- Use ref() for dbt models, source() for external tables.
-- Example: select * from source('bronze', 'your_table_name') once you have a table in sources.yml

{{ config(materialized='view') }}

select
  1 as id,
  'bronze' as layer,
  current_timestamp() as _loaded_at

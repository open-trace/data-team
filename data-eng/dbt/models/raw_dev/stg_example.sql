-- Example raw_dev staging model. Replace with real source tables and logic.
-- Reads from landing / raw_dev; runs when: dbt run --target raw_dev
-- Use ref() for dbt models, source() for external tables.

{{ config(materialized='view') }}

select
  1 as id,
  'raw_dev' as layer,
  current_timestamp() as _loaded_at

-- Example mart_dev model. Replace with refs and aggregations.
-- Runs when: dbt run --target mart_dev

{{ config(materialized='view') }}

select
  1 as id,
  'mart_dev' as layer,
  current_timestamp() as _loaded_at

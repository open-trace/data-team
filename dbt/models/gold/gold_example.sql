-- Example gold model. Replace with refs to silver/gold and aggregations.
-- Runs when: dbt run --target gold

{{ config(materialized='view') }}

select
  1 as id,
  'gold' as layer,
  current_timestamp() as _loaded_at

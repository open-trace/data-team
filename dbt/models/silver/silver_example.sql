-- Example silver model. Replace with real refs to bronze/silver and logic.
-- Runs when: dbt run --target silver

{{ config(materialized='view') }}

-- Placeholder: once bronze models exist, use ref('stg_example').
select
  1 as id,
  'silver' as layer,
  current_timestamp() as _loaded_at

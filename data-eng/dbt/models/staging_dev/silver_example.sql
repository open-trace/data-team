-- Example staging_dev model. Replace with refs to raw_dev / sources and logic.
-- Runs when: dbt run --target staging_dev

{{ config(materialized='view') }}

-- Placeholder: use ref('stg_example') when wired to real upstream models.
select
  1 as id,
  'staging_dev' as layer,
  current_timestamp() as _loaded_at

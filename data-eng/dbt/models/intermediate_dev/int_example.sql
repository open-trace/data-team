-- int_example.sql is retired.
-- The intermediate_dev layer is now organised into domain subfolders:
--
--   faostat/int_faostat_production.sql     → feeds fact_production → fct_production
--   faostat/int_faostat_land_inputs.sql    → feeds fact_land_use   → fct_land_use
--   fews/int_fews_food_security.sql        → feeds fact_humanitarian → fct_humanitarian
--   fews/int_fews_market_prices.sql        → feeds fact_market_access → fct_market_access
--   climate/int_climate_observations.sql   → feeds fact_climate → fct_climate
--
-- This file is kept as a placeholder comment only and is intentionally disabled.
-- Do not reference this model from any downstream model.

{{ config(enabled=false) }}

select 1 as _placeholder


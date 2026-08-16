{{ config(materialized='table') }}

-- intermediate_dev: FEWS market prices domain.
-- Reads: stg_fews_market_prices (FEWS_NET_market_Prices_time_series_data,
--        already cleaned and year/month extracted in staging).
-- Feeds: stg_silver_star_metrics spine → fact_market_access → fct_market_access (Gold).
--
-- Spine mapping:
--   domain_name    = 'market_access'
--   source_name    = source_natural_key  ('FEWS_NET_market_Prices')
--   country_code   = country_code        (FEWS ISO-2)
--   country_name   = country
--   admin_region   = admin_1             (sub-national market location)
--   period_year    = year                (extracted from period_date in staging)
--   indicator_name = price_type          (retail / wholesale / farm-gate)
--   unit_name      = unit                (e.g. 'KG', '90 KG BAG')
--   metric_value   = value               (observed market price)
--   crop_name      = product_name        (commodity traded, e.g. 'Maize')
--   all other spine columns = null

select
    'market_access'                         as domain_name,
    source_natural_key                      as source_name,
    country_code,
    country                                 as country_name,
    admin_1                                 as admin_region,
    cast(year as float64)                   as period_year,
    price_type                              as indicator_name,
    unit                                    as unit_name,
    cast(value as float64)                  as metric_value,
    product_name                            as crop_name,
    cast(null as string)                    as season_name,
    cast(null as string)                    as practice_name,
    cast(null as string)                    as technology_name,
    cast(null as string)                    as ipc_phase_name,
    cast(null as float64)                   as ipc_phase_numeric,
    cast(null as string)                    as shock_type_name,
    cast(null as string)                    as policy_area_name,
    cast(null as string)                    as value_chain_stage_name,
    source_natural_key,
    loaded_at

from {{ ref('stg_fews_market_prices') }}
where value is not null

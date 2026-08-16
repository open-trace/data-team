{{ config(materialized='table') }}

-- =============================================================
-- mart_food_security
-- Gold layer: food security dashboard table for Power BI / Looker.
-- Joins nutrition + humanitarian facts with country and period dims.
-- Aggregated to country / year / indicator grain.
-- =============================================================

with nutrition as (
    select
        f.country_key,
        f.period_key,
        f.indicator_key,
        f.source_key,
        f.unit_key,
        f.nutrition_value             as metric_value,
        'nutrition'                   as domain_name
    from {{ ref('fact_nutrition') }} f
),

humanitarian as (
    select
        f.country_key,
        f.period_key,
        f.indicator_key,
        f.source_key,
        f.unit_key,
        f.humanitarian_value          as metric_value,
        'humanitarian'                as domain_name
    from {{ ref('fact_humanitarian') }} f
),

combined as (
    select * from nutrition
    union all
    select * from humanitarian
),

enriched as (
    select
        c.country_code,
        c.country_name,
        p.period_year,
        i.indicator_name,
        s.source_name,
        u.unit_name,
        m.domain_name,
        sum(m.metric_value)           as total_value,
        avg(m.metric_value)           as avg_value,
        count(*)                      as record_count
    from combined m
    left join {{ ref('dim_country') }}    c on m.country_key   = c.country_key
    left join {{ ref('dim_period') }}     p on m.period_key    = p.period_key
    left join {{ ref('dim_indicator') }}  i on m.indicator_key = i.indicator_key
    left join {{ ref('dim_source') }}     s on m.source_key    = s.source_key
    left join {{ ref('dim_unit') }}       u on m.unit_key      = u.unit_key
    group by 1, 2, 3, 4, 5, 6, 7
)

select
    to_hex(md5(concat(
        coalesce(country_code, ''),  '|',
        coalesce(cast(period_year as string), ''), '|',
        coalesce(indicator_name, ''), '|',
        coalesce(domain_name, '')
    )))                               as mart_food_security_key,
    country_code,
    country_name,
    period_year,
    indicator_name,
    source_name,
    unit_name,
    domain_name,
    total_value,
    avg_value,
    record_count,
    current_timestamp()               as _refreshed_at
from enriched
where country_code is not null
  and period_year  is not null

{{ config(materialized='table') }}

-- FAOSTAT wide trial + soil summaries for exploratory joins (FAO metrics need domain mapping refinement).
-- Writes to staging_dev per team naming convention.

with faostat_unpivoted as (
    select
        cast(area_code_m49 as string) as country_code,
        area as country_name,
        cast(replace(year_label, 'y', '') as int64) as year,
        lower(coalesce(item, '')) as item_lc,
        lower(coalesce(element, '')) as element_lc,
        cast(year_value as float64) as metric_value
    from {{ source('landing', 'faostat_africa_bulk_trial') }}
    unpivot (year_value for year_label in (
        y1961, y1962, y1963, y1964, y1965, y1966, y1967, y1968, y1969, y1970,
        y1971, y1972, y1973, y1974, y1975, y1976, y1977, y1978, y1979, y1980,
        y1981, y1982, y1983, y1984, y1985, y1986, y1987, y1988, y1989, y1990,
        y1991, y1992, y1993, y1994, y1995, y1996, y1997, y1998, y1999, y2000,
        y2001, y2002, y2003, y2004, y2005, y2006, y2007, y2008, y2009, y2010,
        y2011, y2012, y2013, y2014, y2015, y2016, y2017, y2018, y2019, y2020,
        y2021, y2022, y2023, y2024
    ))
    where area_code_m49 is not null
      and year_value is not null
),

fao_land_use as (
    select
        country_code,
        country_name,
        year,
        sum(metric_value) as land_use_value_sum
    from faostat_unpivoted
    where regexp_contains(item_lc, r'land')
       or regexp_contains(element_lc, r'land')
    group by 1, 2, 3
),

fao_fertilizer as (
    select
        country_code,
        country_name,
        year,
        sum(metric_value) as fertilizer_value_sum
    from faostat_unpivoted
    where regexp_contains(item_lc, r'fertili')
       or regexp_contains(element_lc, r'fertili')
    group by 1, 2, 3
),

fao_pesticides as (
    select
        country_code,
        country_name,
        year,
        sum(metric_value) as pesticide_value_sum
    from faostat_unpivoted
    where regexp_contains(item_lc, r'pesticide')
       or regexp_contains(element_lc, r'pesticide')
    group by 1, 2, 3
),

fao_country_year as (
    select distinct
        country_code,
        country_name,
        year
    from faostat_unpivoted
),

fao_joined as (
    select
        b.country_code,
        b.country_name,
        b.year,
        l.land_use_value_sum,
        f.fertilizer_value_sum,
        p.pesticide_value_sum
    from fao_country_year b
    left join fao_land_use l
        on b.country_code = l.country_code
       and b.year = l.year
    left join fao_fertilizer f
        on b.country_code = f.country_code
       and b.year = f.year
    left join fao_pesticides p
        on b.country_code = p.country_code
       and b.year = p.year
),

isda_geo as (
    select
        round(cast(latitude as numeric), 3) as lat_3,
        round(cast(longitude as numeric), 3) as lon_3,
        avg(cast(value as float64)) as isda_avg_value,
        count(*) as isda_points
    from {{ source('landing', 'isda_soil_bulk_master') }}
    where latitude is not null and longitude is not null
    group by 1, 2
),

isric_geo as (
    select
        round(cast(latitude as numeric), 3) as lat_3,
        round(cast(longitude as numeric), 3) as lon_3,
        avg(cast(soc_0_5cm as float64)) as isric_avg_soc_0_5cm,
        avg(cast(phh2o_0_5cm as float64)) as isric_avg_ph_0_5cm,
        avg(cast(clay_0_5cm as float64)) as isric_avg_clay_0_5cm,
        count(*) as isric_points
    from {{ source('landing', 'isric_africa_soil_data') }}
    where latitude is not null and longitude is not null
    group by 1, 2
),

soil_joined as (
    select
        coalesce(i.lat_3, r.lat_3) as lat_3,
        coalesce(i.lon_3, r.lon_3) as lon_3,
        i.isda_avg_value,
        i.isda_points,
        r.isric_avg_soc_0_5cm,
        r.isric_avg_ph_0_5cm,
        r.isric_avg_clay_0_5cm,
        r.isric_points
    from isda_geo i
    full outer join isric_geo r
        on i.lat_3 = r.lat_3
       and i.lon_3 = r.lon_3
),

soil_summary as (
    select
        avg(isda_avg_value) as soil_isda_avg_value,
        avg(isric_avg_soc_0_5cm) as soil_isric_avg_soc_0_5cm,
        avg(isric_avg_ph_0_5cm) as soil_isric_avg_ph_0_5cm,
        avg(isric_avg_clay_0_5cm) as soil_isric_avg_clay_0_5cm,
        count(*) as soil_joined_geopoints
    from soil_joined
)

select
    f.country_code,
    f.country_name,
    f.year,
    f.land_use_value_sum,
    f.fertilizer_value_sum,
    f.pesticide_value_sum,
    s.soil_isda_avg_value,
    s.soil_isric_avg_soc_0_5cm,
    s.soil_isric_avg_ph_0_5cm,
    s.soil_isric_avg_clay_0_5cm,
    s.soil_joined_geopoints
from fao_joined f
cross join soil_summary s

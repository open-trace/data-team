{{ config(materialized='table') }}

with isda as (
    select
        round(
            cast(
                (atan(sinh(cast(latitude as float64) / 6378137.0)) * 180.0 / 3.141592653589793) as numeric
            ),
            2
        ) as lat_2,
        round(cast((cast(longitude as float64) / 6378137.0) * 180.0 / 3.141592653589793 as numeric), 2) as lon_2,
        lower(cast(property as string)) as property_name,
        cast(depth as string) as depth_label,
        cast(value as float64) as property_value
    from {{ source('landing', 'isda_soil_bulk_master') }}
    where latitude is not null
      and longitude is not null
      and value is not null
      and cast(latitude as float64) between -20037508.34 and 20037508.34
      and cast(longitude as float64) between -20037508.34 and 20037508.34
),

isda_agg as (
    select
        lat_2,
        lon_2,
        count(*) as isda_points,
        avg(property_value) as isda_avg_value_all,
        avg(case when property_name like '%carbon%' then property_value end) as isda_avg_carbon,
        avg(case when property_name like '%nitrogen%' then property_value end) as isda_avg_nitrogen,
        avg(case when property_name in ('ph', 'phh2o') or property_name like '%ph%' then property_value end) as isda_avg_ph
    from isda
    group by 1, 2
),

isric as (
    select
        round(cast(latitude as numeric), 2) as lat_2,
        round(cast(longitude as numeric), 2) as lon_2,
        cast(soc_0_5cm as float64) as soc_0_5cm,
        cast(phh2o_0_5cm as float64) as phh2o_0_5cm,
        cast(clay_0_5cm as float64) as clay_0_5cm,
        cast(nitrogen_0_5cm as float64) as nitrogen_0_5cm,
        cast(sand_0_5cm as float64) as sand_0_5cm,
        cast(silt_0_5cm as float64) as silt_0_5cm
    from {{ source('landing', 'isric_africa_soil_data') }}
    where latitude is not null
      and longitude is not null
),

isric_agg as (
    select
        lat_2,
        lon_2,
        count(*) as isric_points,
        avg(soc_0_5cm) as isric_avg_soc_0_5cm,
        avg(phh2o_0_5cm) as isric_avg_ph_0_5cm,
        avg(clay_0_5cm) as isric_avg_clay_0_5cm,
        avg(nitrogen_0_5cm) as isric_avg_nitrogen_0_5cm,
        avg(sand_0_5cm) as isric_avg_sand_0_5cm,
        avg(silt_0_5cm) as isric_avg_silt_0_5cm
    from isric
    group by 1, 2
)

select
    coalesce(i.lat_2, r.lat_2) as lat_2,
    coalesce(i.lon_2, r.lon_2) as lon_2,
    i.isda_points,
    i.isda_avg_value_all,
    i.isda_avg_carbon,
    i.isda_avg_nitrogen,
    i.isda_avg_ph,
    r.isric_points,
    r.isric_avg_soc_0_5cm,
    r.isric_avg_ph_0_5cm,
    r.isric_avg_clay_0_5cm,
    r.isric_avg_nitrogen_0_5cm,
    r.isric_avg_sand_0_5cm,
    r.isric_avg_silt_0_5cm
from isda_agg i
full outer join isric_agg r
    on i.lat_2 = r.lat_2
   and i.lon_2 = r.lon_2

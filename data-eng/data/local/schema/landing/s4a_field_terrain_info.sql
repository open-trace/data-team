CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."s4a_field_terrain_info" (
  "boundedby" TEXT,
  "plot_code" TEXT,
  "country_code" TEXT,
  "survey_date" TEXT,
  "altitude" TEXT,
  "longitude" TEXT,
  "latitude" TEXT,
  "tsu_id" TEXT,
  "plot_id" TEXT,
  "obstruct_lyr" TEXT,
  "depth_restriction_cause" TEXT,
  "country_name" TEXT,
  "srf_stone" TEXT,
  "lndform" TEXT,
  "slope_class" TEXT,
  "slope_position" TEXT,
  "slope_pathway" TEXT
);

CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."s4a_field_grazing_info" (
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
  "grazing_sign_present" TEXT,
  "grazing_sign_infrastructure" TEXT,
  "grazing_sign_droppings" TEXT,
  "grazing_sign_grass" TEXT,
  "overgrazing_sign_bare" TEXT,
  "overgrazing_sign_dung" TEXT,
  "overgrazing_sign_deforest" TEXT
);

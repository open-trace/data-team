-- Restore bronze and silver tables using BigQuery time travel.
-- Run in BigQuery console or via bq query. Replace the timestamp if needed.
-- Project: opentrace-prod-5ga4. Time-travel point: 2026-03-08T22:14:26.677681+00:00

-- ========== BRONZE (22 tables) ==========

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.OECD_Food_data_Africa_NEW` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.OECD_Food_data_Africa_NEW`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.WFP_VAMPIRE_Tool_global_food_prices` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.WFP_VAMPIRE_Tool_global_food_prices`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.africa_Human_development_index` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.africa_Human_development_index`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.africa_gdp_ppp` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.africa_gdp_ppp`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.africa_nasa_power_bronze` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.africa_nasa_power_bronze`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.africa_soil_moisture` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.africa_soil_moisture`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.climatewatch_emission_pathways` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.climatewatch_emission_pathways`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.cropland_area_summary_2019_africa` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.cropland_area_summary_2019_africa`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.fao_rfn` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.fao_rfn`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.fao_rl` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.fao_rl`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.fao_rp` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.fao_rp`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.fao_tcl` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.fao_tcl`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.fao_ti` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.fao_ti`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.fews_net_food_security_master` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.fews_net_food_security_master`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.gbif_occurrence_search` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.gbif_occurrence_search`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.ifpri_africa_bronze` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.ifpri_africa_bronze`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.isda_soil_bulk_master` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.isda_soil_bulk_master`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.isda_soil_bulk_master_bronze` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.isda_soil_bulk_master_bronze`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.openaire_data_sources_bronze` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.openaire_data_sources_bronze`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.openaire_full_bronze` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.openaire_full_bronze`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.openaire_organizations_bronze` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.openaire_organizations_bronze`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.bronze.yield_raw_data` AS
SELECT * FROM `opentrace-prod-5ga4.bronze.yield_raw_data`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

-- ========== SILVER (19 tables) ==========

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.Silver_raw_data` AS
SELECT * FROM `opentrace-prod-5ga4.silver.Silver_raw_data`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.africa_cropland_summary_2019` AS
SELECT * FROM `opentrace-prod-5ga4.silver.africa_cropland_summary_2019`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.africa_nasa_power_silver` AS
SELECT * FROM `opentrace-prod-5ga4.silver.africa_nasa_power_silver`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fao_fbs` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fao_fbs`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fao_fbs_view` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fao_fbs_view`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fao_qcl` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fao_qcl`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fao_qcl_1` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fao_qcl_1`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fao_qi` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fao_qi`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fao_qv` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fao_qv`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fao_rhn` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fao_rhn`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fao_rl_landuse` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fao_rl_landuse`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fao_rp_pesticides` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fao_rp_pesticides`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fao_tcl_crop_and_pest` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fao_tcl_crop_and_pest`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fao_ti_trade_indices` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fao_ti_trade_indices`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fews_net_food_security_master_alldata_silver` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fews_net_food_security_master_alldata_silver`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.fews_net_food_security_master_silver` AS
SELECT * FROM `opentrace-prod-5ga4.silver.fews_net_food_security_master_silver`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.isda_soil_bulk_refined` AS
SELECT * FROM `opentrace-prod-5ga4.silver.isda_soil_bulk_refined`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.openaire_data_sources_silver` AS
SELECT * FROM `opentrace-prod-5ga4.silver.openaire_data_sources_silver`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

CREATE OR REPLACE TABLE `opentrace-prod-5ga4.silver.yield_raw_data_silver` AS
SELECT * FROM `opentrace-prod-5ga4.silver.yield_raw_data_silver`
FOR SYSTEM_TIME AS OF '2026-03-08T22:14:26.677681+00:00';

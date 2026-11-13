CREATE SCHEMA IF NOT EXISTS "raw_dev";
CREATE TABLE IF NOT EXISTS "raw_dev"."ilri_05_nutrient_composition" (
  "impact_expanded_naming" TEXT,
  "dry_matter_as_fed" TEXT,
  "crude_protein_dm_avg" TEXT,
  "gross_energy_mj_kg_dm_avg" TEXT,
  "om_digestibility_ruminant_avg" TEXT,
  "energy_digestibility_growing_pig_avg" TEXT,
  "cppf_cp_as_fed_calculated" TEXT,
  "source_of_dm_data" TEXT
);

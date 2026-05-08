from __future__ import annotations

from pathlib import Path

import yaml


TABLE_NAMES = [
    "FEWS_NET_Food_insecure_population_estimates_data_series",
    "FEWS_NET_Food_insecure_population_estimates_time_series_data",
    "FEWS_NET_cross_border_Trade_time_series_data",
    "FEWS_NET_cross_border_trade_data_series",
    "FEWS_NET_food_security_classifications_data_series",
    "FEWS_NET_food_security_classifications_time_series_data",
    "FEWS_NET_market_Prices_time_series_data",
    "FEWS_NET_market_prices_data_series",
    "WFP_VAMPIRE_Tool_global_food_prices_bronze",
    "africa_Human_development_index",
    "africa_climate_test_ingest",
    "africa_crop_production_bronze",
    "africa_gross_domestic_product_purchasing_power_parity_bronze",
    "africa_nasa_power_daily_summary_bronze",
    "africa_nasa_power_hourly_bronze",
    "arcgis_infrastructure_tourism_poi",
    "arcgis_land_protected_areas",
    "arcgis_layer_rice_germplasm_in_africa_3d2a9",
    "arcgis_rarity_80_birds_africa",
    "arcgis_south_africa_wards_demographics_2ce07",
    "arcgis_vegetation_ndvi",
    "cifor_icraf_raw",
    "climatewatch_climate_health_impacts",
    "copernicus_climate_raw_era5_stats_2023_06",
    "crop_germplasm_africa",
    "fao_fertilizers_nutrient_bronze",
    "fao_land_use_bronze",
    "fao_pesticides_use_bronze",
    "fao_trade_crops_livestock_bronze",
    "fao_trade_indices_bronze",
    "gbif_biodiversity_occurrence",
    "ilri_crp_household_food_security_v1",
    "ilri_vegetation_survey_v1",
    "isda_soil_property",
    "isric_africa_soil_data",
    "nakuru_air_quality_archive",
    "openaire_agriculture_and_environment_Research_publications_Data_sources_bronze",
    "openaire_agriculture_and_environment_Research_publications_Organizations_bronze",
    "openaire_agriculture_and_environment_Research_publications_Persons_bronze",
    "openaire_agriculture_and_environment_Research_publications_Product_links_bronze",
    "openaire_agriculture_and_environment_Research_publications_Projects_bronze",
    "yield_raw_data",
]


def _out_dir() -> Path:
    return Path(__file__).resolve().parent


def _write_table_yaml(table_name: str, out_path: Path) -> None:
    doc = {
        "name": table_name,
        "description": "TODO: Add a concise human-readable table description.",
        "columns": [
            {
                "name": "TODO_column_name",
                "description": "TODO: Add column description (type, meaning, units, constraints).",
            }
        ],
        "examples": [
            "TODO: Provide 1–3 example questions this table can answer.",
        ],
        "notes": [
            "TODO: Add joins/keys, grain, update frequency, caveats.",
        ],
    }
    out_path.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8")


def main() -> int:
    out_dir = _out_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    n = 0
    for t in TABLE_NAMES:
        out_path = out_dir / f"{t}.yml"
        _write_table_yaml(t, out_path)
        n += 1

    print(f"Wrote {n} YAML files to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


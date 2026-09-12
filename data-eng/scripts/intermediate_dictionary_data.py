"""Curated intermediate_dev entity dictionary seed + shared column descriptions.

Used by intermediate_dictionary_columns.py and the Excel builder.
Entities are built from a SQL/schema scan with optional manual overlays.
"""
from __future__ import annotations

from typing import Any

# Shared column descriptions applied across intermediate models when present.
COMMON_COLUMN_DESC: dict[str, dict[str, str]] = {
    "geo_key": {
        "role": "FK",
        "description": (
            "Conformed geography key linking this row to int_geography_conformed. "
            "Use this join key for place-level analysis; may be null when geo attach fails "
            "(often warn-severity in tests)."
        ),
        "example": "geo_eth_country",
        "data_type": "STRING",
    },
    "geo_level": {
        "role": "dim",
        "description": (
            "Geographic grain of the place row: country, admin1, admin2, city, or fnid. "
            "Filter on geo_level before aggregating across unlike place grains."
        ),
        "example": "country",
        "data_type": "STRING",
    },
    "country_iso3": {
        "role": "dim",
        "description": (
            "ISO 3166-1 alpha-3 country code. Enables consistent country filtering and joins "
            "to int_ref_country / int_geography_conformed even when geo_key is unmatched."
        ),
        "example": "ETH",
        "data_type": "STRING",
    },
    "country_iso2": {
        "role": "dim",
        "description": (
            "ISO 3166-1 alpha-2 country code. Often used as a bridge from FEWS or city sources "
            "before resolving ISO3 via int_ref_country."
        ),
        "example": "ET",
        "data_type": "STRING",
    },
    "country_name": {
        "role": "dim",
        "description": (
            "Human-readable country name as provided by the upstream source or conformed reference. "
            "Prefer country_iso3 for joins; use country_name for display and fuzzy matching."
        ),
        "example": "Ethiopia",
        "data_type": "STRING",
    },
    "country": {
        "role": "dim",
        "description": (
            "Country label from the source feed (may be name or code depending on upstream). "
            "Normalize via int_ref_country or int_geography_conformed before cross-source compares."
        ),
        "example": "Ethiopia",
        "data_type": "STRING",
    },
    "admin_1_name": {
        "role": "dim",
        "description": (
            "First-level administrative division name (state/region/province) when present on the source."
        ),
        "example": "Oromia",
        "data_type": "STRING",
    },
    "admin_2_name": {
        "role": "dim",
        "description": (
            "Second-level administrative division name (district/zone) when present on the source."
        ),
        "example": "East Shewa",
        "data_type": "STRING",
    },
    "city_name": {
        "role": "dim",
        "description": "City or locality name for city-grain geography rows.",
        "example": "Addis Ababa",
        "data_type": "STRING",
    },
    "fnid": {
        "role": "dim",
        "description": (
            "FEWS NET livelihood / admin unit identifier used as a subnational place key. "
            "Join to int_geography_conformed where geo_level = fnid."
        ),
        "example": "ETR103",
        "data_type": "STRING",
    },
    "latitude": {
        "role": "dim",
        "description": "Latitude in decimal degrees for point or city observations.",
        "example": "9.03",
        "data_type": "FLOAT64",
    },
    "longitude": {
        "role": "dim",
        "description": "Longitude in decimal degrees for point or city observations.",
        "example": "38.74",
        "data_type": "FLOAT64",
    },
    "source_natural_key": {
        "role": "dim",
        "description": (
            "Stable natural key for the producing dataset (e.g. faostat_crops, fews_fs). "
            "Joins to int_source_registry for ACF Path B producer metadata (tier, data_level)."
        ),
        "example": "faostat_crops",
        "data_type": "STRING",
    },
    "source_key": {
        "role": "FK",
        "description": (
            "Surrogate or conformed source key used downstream in mart dims; "
            "aligns with lineage carried from intermediate into mart_dev."
        ),
        "example": "src_faostat_prod",
        "data_type": "STRING",
    },
    "area_code": {
        "role": "dim",
        "description": (
            "FAOSTAT area code identifying the geographic reporting unit. "
            "Resolve to ISO via int_faostat_area_reference / int_faostat_area_iso before country joins."
        ),
        "example": "238",
        "data_type": "STRING",
    },
    "item_code": {
        "role": "dim",
        "description": "FAOSTAT item (commodity/product) code for the measure row.",
        "example": "56",
        "data_type": "STRING",
    },
    "element_code": {
        "role": "dim",
        "description": (
            "FAOSTAT element code identifying the measure type (production, area harvested, yield, etc.)."
        ),
        "example": "5510",
        "data_type": "STRING",
    },
    "year": {
        "role": "dim",
        "description": "Calendar year of the observation or reporting period.",
        "example": "2020",
        "data_type": "INT64",
    },
    "month": {
        "role": "dim",
        "description": "Calendar month (1–12) of the observation when the source is monthly.",
        "example": "6",
        "data_type": "INT64",
    },
    "value": {
        "role": "measure",
        "description": (
            "Primary numeric measure for this conformed fact row. Interpret together with unit "
            "and any grain discriminator (production_grain, measure_type, price_source, etc.)."
        ),
        "example": "1000",
        "data_type": "FLOAT64",
    },
    "unit": {
        "role": "measure",
        "description": "Unit of measure for value (tonnes, people, USD, index points, etc.).",
        "example": "tonnes",
        "data_type": "STRING",
    },
    "loaded_at": {
        "role": "meta",
        "description": "Pipeline load timestamp when the intermediate row was materialized.",
        "example": "2026-08-01T00:00:00",
        "data_type": "TIMESTAMP",
    },
    "native_id": {
        "role": "dim",
        "description": (
            "Source-native identifier for the place or entity (city id, FNID, ISO3, etc.) "
            "used when building geo_key uniqueness."
        ),
        "example": "1840015588",
        "data_type": "STRING",
    },
    "in_africa_scope": {
        "role": "dim",
        "description": (
            "Boolean flag indicating the country is in OpenTrace Africa analysis scope "
            "(from int_ref_country / M49 + geo seed)."
        ),
        "example": "true",
        "data_type": "BOOL",
    },
    "production_grain": {
        "role": "dim",
        "description": (
            "Discriminator for FAOSTAT production long-form rows: physical, index, or gross_value. "
            "Filter production_grain before comparing quantities across elements."
        ),
        "example": "physical",
        "data_type": "STRING",
    },
    "measure_type": {
        "role": "dim",
        "description": (
            "Food-security product type: population counts versus classification phases. "
            "Never union population and classification metrics without an explicit grain caveat."
        ),
        "example": "population",
        "data_type": "STRING",
    },
    "price_value": {
        "role": "measure",
        "description": "Observed or reported market price amount after harmonisation.",
        "example": "42.5",
        "data_type": "FLOAT64",
    },
    "price_source": {
        "role": "dim",
        "description": (
            "Price feed origin after harmonisation: fews, wfp, or faostat. "
            "Keep price_source when comparing volatility across markets."
        ),
        "example": "fews",
        "data_type": "STRING",
    },
    "measurement_form": {
        "role": "dim",
        "description": (
            "Macro indicator form (share_of_gdp, annual_growth, constant_price, current_price). "
            "Filter before charting FAOSTAT macro series."
        ),
        "example": "share_of_gdp",
        "data_type": "STRING",
    },
    "soil_property": {
        "role": "dim",
        "description": "Soil attribute name in long-form soil tables (e.g. organic carbon, pH, texture class).",
        "example": "soc",
        "data_type": "STRING",
    },
    "depth_band": {
        "role": "dim",
        "description": "Soil sampling depth interval label for the property value.",
        "example": "0-20cm",
        "data_type": "STRING",
    },
    "season_key": {
        "role": "FK",
        "description": "Normalized season identifier for yield/season calendars.",
        "example": "main",
        "data_type": "STRING",
    },
    "season_country_key": {
        "role": "FK",
        "description": "Composite season × country key used to attach yield rows to int_season_mapped.",
        "example": "ETH-main",
        "data_type": "STRING",
    },
    "season_name": {
        "role": "dim",
        "description": "Human-readable agricultural season name from the yield source.",
        "example": "Main",
        "data_type": "STRING",
    },
    "tier": {
        "role": "acf",
        "description": "ACF producer-scale tier (1=global, 2=national ministry, 3=community survey).",
        "example": "1",
        "data_type": "INT64",
    },
    "data_level": {
        "role": "acf",
        "description": "ACF warehouse resolution of the number (global, national, sub_national, community, point).",
        "example": "national",
        "data_type": "STRING",
    },
    "organisation_name": {
        "role": "dim",
        "description": "Producing organisation name in the source registry.",
        "example": "FAO",
        "data_type": "STRING",
    },
    "default_data_level": {
        "role": "acf",
        "description": "Default ACF data_level for this source_natural_key in int_source_registry.",
        "example": "national",
        "data_type": "STRING",
    },
    "producer_scale": {
        "role": "acf",
        "description": "Producer scale band (global, regional, national, subnational) for ACF scoring.",
        "example": "global",
        "data_type": "STRING",
    },
    "match_method": {
        "role": "dim",
        "description": (
            "How FAOSTAT area_code was mapped to a country: m49, ref_country_name, or alias. "
            "Null when unmatched."
        ),
        "example": "m49",
        "data_type": "STRING",
    },
    "product": {
        "role": "dim",
        "description": "Crop or commodity name from the yield/production source feed.",
        "example": "Maize",
        "data_type": "STRING",
    },
    "population": {
        "role": "measure",
        "description": "Population count associated with the place or survey unit when provided by the source.",
        "example": "120000",
        "data_type": "INT64",
    },
}

# Optional per-table column overlays: table -> col -> {role, description, example, data_type}
COLUMN_OVERRIDES: dict[str, dict[str, dict[str, str]]] = {
    "int_geography_conformed": {
        "geo_key": {
            "role": "PK",
            "description": (
                "Primary place key for the OpenTrace intermediate geography spine. "
                "Uniqueness is enforced with source_natural_key and native_id. "
                "All with_geo intermediate facts should join here on geo_key."
            ),
            "example": "geo_eth_fnid_ETR103",
        },
    },
    "int_source_registry": {
        "source_natural_key": {
            "role": "PK",
            "description": (
                "Primary key of the ACF Path B source registry. One row per producing dataset "
                "with organisation, tier, default_data_level, and producer_scale."
            ),
            "example": "faostat_crops",
        },
    },
    "int_ref_country": {
        "country_iso3": {
            "role": "PK",
            "description": (
                "Canonical ISO3 country code for Africa-scoped reference countries "
                "(stg_geo + M49 seed). Use as the authoritative country list for filters."
            ),
            "example": "KEN",
        },
    },
}

# Manual entity overlays merged onto auto-scanned models (optional fields only).
ENTITY_OVERLAYS: dict[str, dict[str, Any]] = {
    "int_geography_conformed": {
        "grain": "One row per place × source_natural_key × native_id",
        "primary_key": "geo_key",
        "joins": "Downstream facts join on geo_key; upstream cities/FEWS/yield/FAOSTAT geo extracts",
    },
    "int_source_registry": {
        "grain": "One row per source_natural_key",
        "primary_key": "source_natural_key",
        "joins": "Facts carry source_natural_key → this registry for ACF metadata",
    },
    "int_ref_country": {
        "grain": "One row per country_iso3",
        "primary_key": "country_iso3",
        "joins": "Used by geography and area_code ISO resolution",
    },
    "int_faostat_production_conformed": {
        "grain": "area_code × item × element × year × source_natural_key",
        "primary_key": "composite (area_code, item_code, element_code, year, source_natural_key)",
        "joins": "area_code → int_faostat_area_*; source_natural_key → int_source_registry",
    },
}


def humanize_identifier(name: str) -> str:
    parts = [p for p in str(name or "").replace("-", "_").split("_") if p]
    if not parts:
        return "field"
    return " ".join(parts)


def generate_column_description(
    *,
    table_name: str,
    column_name: str,
    domain: str,
    table_purpose: str,
    role: str,
) -> str:
    """Detailed fallback prose when no curated description exists."""
    label = humanize_identifier(column_name)
    purpose = (table_purpose or "").strip().rstrip(".")
    domain_l = humanize_identifier(domain) if domain else "intermediate"
    role_bit = {
        "PK": "This is the primary key for the model.",
        "FK": "This is a foreign/join key used to relate rows to a spine or companion intermediate table.",
        "measure": "This is a numeric measure field; interpret with unit and grain discriminators on the same row.",
        "meta": "This is pipeline metadata rather than an analytical measure.",
        "acf": "This field supports ACF Path B evidence scoring and lineage.",
        "dim": "This is a descriptive or classificatory attribute on the entity.",
    }.get(role, "This attribute describes the entity.")

    return (
        f"The {label} field on {table_name} ({domain_l} domain). "
        f"{role_bit} "
        f"In this intermediate model — {purpose or 'a conformed OpenTrace intermediate entity'} — "
        f"{label} carries the source or derived value needed for joins, filters, or measures "
        f"before mart_dev facts are built. Prefer documented grain keys "
        f"(geo_key, area_code, source_natural_key, year) when combining with other tables."
    )


def seed_as_dict(entities: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "layer": "intermediate_dev",
        "version": "2026.9",
        "entity_count": len(entities),
        "entities": entities,
        "common_columns": {
            k: {"role": v.get("role"), "description": v.get("description")}
            for k, v in COMMON_COLUMN_DESC.items()
        },
        "column_overrides": COLUMN_OVERRIDES,
    }


__all__ = [
    "COMMON_COLUMN_DESC",
    "COLUMN_OVERRIDES",
    "ENTITY_OVERLAYS",
    "generate_column_description",
    "humanize_identifier",
    "seed_as_dict",
]

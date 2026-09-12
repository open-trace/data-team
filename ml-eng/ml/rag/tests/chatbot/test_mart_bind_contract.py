"""Property tests for TableBindContract — table × facet binding, not class-by-class."""
from __future__ import annotations

import pytest

from ml.rag.chatbot.bq_table_schema_yaml import (
    _AFRICA_COUNTRY_ISO3,
    compile_table_bind_contract,
    geo_column,
    list_mart_table_index,
    resolve_geo_filter_values,
    resolve_geo_literals_for_table,
    year_column,
)
from ml.rag.chatbot.class_engines.prod import ProdEngine
from ml.rag.chatbot.schema_card import load_schema_card


@pytest.mark.parametrize("iso3", sorted(set(_AFRICA_COUNTRY_ISO3.values()))[:12])
def test_iso3_maps_to_country_name_on_agg_production_annual(iso3: str) -> None:
    literals = resolve_geo_literals_for_table("agg_production_annual", [iso3])
    assert literals
    assert literals[0] != iso3
    assert len(literals[0]) > 3


def test_resolve_geo_filter_values_kenya_rice_table() -> None:
    assert resolve_geo_filter_values("agg_production_annual", ["KEN"]) == ["Kenya"]
    assert resolve_geo_filter_values("agg_production_country_year", ["KEN"]) == ["KEN"]


def test_compile_table_bind_contract_has_nomenclature() -> None:
    facets = {
        "geography": ["Kenya"],
        "entities": ["Rice"],
        "time_start": "2016-01-01",
        "time_end": "2016-12-31",
        "primary_measures": ["production"],
    }
    contract = compile_table_bind_contract(
        "agg_production_country_year",
        facets=facets,
        card=load_schema_card("PROD") or {},
        query="what is the production of rice in kenya in 2016",
        country_labels=["KEN"],
    )
    assert contract.geo_column == "country_iso3"
    assert "KEN" in contract.geo_literals
    assert contract.time_column == "year"
    assert "TABLE:" in contract.nomenclature
    assert "country_iso3" in contract.nomenclature


def test_agg_production_annual_uses_time_key_not_year() -> None:
    assert year_column("agg_production_annual") == "time_key"
    contract = compile_table_bind_contract(
        "agg_production_annual",
        facets={"time_start": "2016-01-01", "time_end": "2016-12-31", "geography": ["Kenya"]},
        query="production kenya 2016",
        country_labels=["KEN"],
    )
    assert contract.time_column == "time_key"
    assert any("time_key" in (contract.time_sql or "") for _ in [0]) or contract.time_sql


@pytest.mark.parametrize(
    "table_id",
    [
        tid
        for tid in ("agg_production_country_year", "agg_production_annual", "fct_production")
        if tid in {r["table_id"] for r in list_mart_table_index()}
    ],
)
def test_geo_column_exists_in_yaml(table_id: str) -> None:
    col = geo_column(table_id)
    assert col is not None


def test_compile_table_bind_contract_includes_bind_spine_nomenclature() -> None:
    contract = compile_table_bind_contract(
        "fct_land_inputs",
        facets={"geography": ["Kenya"], "time_start": "2016-01-01", "time_end": "2016-12-31"},
        query="fertilizer use in kenya 2016",
        country_labels=["KEN"],
    )
    assert contract.bind_spine.get("geography_key") == "dim_geography.country_iso3"
    assert contract.bind_spine.get("item_key") == "dim_item.item_name"
    assert contract.bind_spine.get("element_key") == "dim_element.element_name"
    assert contract.bind_spine.get("source_key") == "dim_source.organisation_name"
    assert "JOIN_SPINE" in contract.nomenclature


def test_fct_land_use_bind_spine_maps_land_use_key() -> None:
    contract = compile_table_bind_contract(
        "fct_land_use",
        facets={"geography": ["Kenya"], "time_start": "2016-01-01", "time_end": "2016-12-31"},
        query="cropland area in kenya 2016",
        country_labels=["KEN"],
    )
    assert contract.bind_spine.get("land_use_key") == "dim_land_use.land_use_code"
    assert contract.bind_spine.get("source_key") == "dim_source.organisation_name"


def test_fct_land_use_spine_entity_binds_cropland() -> None:
    contract = compile_table_bind_contract(
        "fct_land_use",
        facets={"entities": ["cropland"], "time_start": "2016-01-01", "time_end": "2016-12-31"},
        query="cropland area in kenya",
        country_labels=["KEN"],
    )
    assert any(fk == "land_use_key" for fk, _ in contract.spine_entity_filters)
    assert "land_use_key" in contract.required_filters_sql
    assert "dim_land_use" in contract.required_filters_sql
    assert "SPINE_ENTITY: land_use_key" in contract.nomenclature


def test_fct_food_hazards_spine_entity_binds_disease() -> None:
    contract = compile_table_bind_contract(
        "fct_food_hazards",
        facets={"entities": ["salmonella"], "time_start": "2016-01-01", "time_end": "2016-12-31"},
        query="salmonella food hazard samples in kenya",
        country_labels=["KEN"],
    )
    assert any(fk == "disease_key" for fk, _ in contract.spine_entity_filters)
    assert "disease_key" in contract.required_filters_sql
    assert "dim_disease" in contract.required_filters_sql
    assert "SPINE_ENTITY: disease_key" in contract.nomenclature


def test_spine_entity_skips_noisy_fks_on_production_panel() -> None:
    """Short/noisy dims (unit/sex/household) must not bind from production speech."""
    contract = compile_table_bind_contract(
        "agg_production_country_year",
        facets={
            "geography": ["Kenya"],
            "entities": ["rice", "production"],
            "time_start": "2016-01-01",
            "time_end": "2016-12-31",
            "primary_measures": ["production"],
        },
        query="production of rice in kenya 2016",
        country_labels=["KEN"],
    )
    noisy = {"unit_key", "sex_key", "household_key", "organisation_key", "person_key"}
    bound = {fk for fk, _ in contract.spine_entity_filters}
    assert not (bound & noisy)


def test_legacy_geo_key_binds_via_dim_geography() -> None:
    for table_id in ("fct_land_use", "fct_fertilizer"):
        contract = compile_table_bind_contract(
            table_id,
            facets={"geography": ["Kenya"], "time_start": "2016-01-01", "time_end": "2016-12-31"},
            query="land use in kenya 2016",
            country_labels=["KEN"],
        )
        assert contract.geo_column == "geo_key"
        sql = contract.required_filters_sql
        assert "geo_key IN (SELECT geography_key FROM" in sql
        assert "dim_geography" in sql
        assert "geo_key = '" not in sql
        assert contract.bind_spine.get("geo_key") == "dim_geography.country_iso3"


def test_dim_context_in_nomenclature_for_geography_and_product() -> None:
    contract = compile_table_bind_contract(
        "agg_production_country_year",
        facets={"geography": ["Kenya"], "entities": ["rice"], "primary_measures": ["production"]},
        query="production of rice in kenya",
        country_labels=["KEN"],
    )
    nom = contract.nomenclature
    assert "DIM_CONTEXT: dim_geography SELECT" in nom
    assert "admin_1_name" in nom
    assert "population" in nom
    assert "DIM_CONTEXT: dim_product SELECT" in nom
    assert "DIM_GEO_HINT:" in nom

    land = compile_table_bind_contract(
        "fct_land_use",
        facets={"geography": ["Kenya"], "entities": ["cropland"]},
        query="cropland in kenya",
        country_labels=["KEN"],
    )
    assert "DIM_CONTEXT: dim_geography SELECT" in land.nomenclature
    assert "DIM_CONTEXT: dim_land_use SELECT" in land.nomenclature
    assert "land_use_class" in land.nomenclature


def test_subnational_geo_bind_admin1_in_dim_subquery() -> None:
    contract = compile_table_bind_contract(
        "fct_land_use",
        facets={"geography": ["Kenya"], "entities": ["KwaZulu-Natal"], "time_start": "2016-01-01"},
        query="land use in KwaZulu-Natal kenya",
        country_labels=["KEN"],
    )
    sql = contract.required_filters_sql
    assert "admin_1_name = 'KwaZulu-Natal'" in sql or 'admin_1_name = "KwaZulu-Natal"' in sql
    assert "geo_key IN (SELECT geography_key FROM" in sql
    assert "geo_key = '" not in sql


def test_subnational_geo_bind_city_on_production_panel() -> None:
    contract = compile_table_bind_contract(
        "agg_production_country_year",
        facets={"geography": ["Kenya"], "entities": ["Alexandria"], "primary_measures": ["production"]},
        query="production near Alexandria kenya",
        country_labels=["KEN"],
    )
    sql = contract.required_filters_sql
    assert "country_iso3 = 'KEN'" in sql
    assert "city_name = 'Alexandria'" in sql
    assert "geography_key IN (SELECT geography_key FROM" in sql


def test_pack_mart_hints_includes_spine_dims() -> None:
    from ml.rag.chatbot.bq_table_schema_yaml import pack_mart_table_hints

    hints, _truncated = pack_mart_table_hints(
        ["agg_production_country_year"],
        max_bytes=12000,
        query_terms=["kenya", "rice"],
    )
    joined = "\n".join(hints)
    assert "dim_geography" in joined.lower() or any("dim_geography" in h for h in hints)
    assert "dim_product" in joined.lower() or any("dim_product" in h for h in hints)
    assert "admin_1_name" in joined or "population" in joined


def test_entity_roles_prefer_land_use_over_speech_blob() -> None:
    contract = compile_table_bind_contract(
        "fct_land_use",
        facets={
            "entities": ["area"],
            "entity_roles": {"land_use": ["cropland"]},
            "time_start": "2016-01-01",
            "time_end": "2016-12-31",
        },
        query="area statistics in kenya",
        country_labels=["KEN"],
    )
    assert any(fk == "land_use_key" and "cropland" in lab.lower() for fk, lab in contract.spine_entity_filters)


def test_compile_table_bind_contract_strips_measure_from_entities() -> None:
    """Measure tokens in entities must not become product_name filters."""
    facets = {
        "geography": ["Kenya"],
        "entities": ["production", "rice"],
        "time_start": "2016-01-01",
        "time_end": "2016-12-31",
        "primary_measures": ["production"],
    }
    contract = compile_table_bind_contract(
        "agg_production_country_year",
        facets=facets,
        query="what is the production of rice in kenya in 2016",
        country_labels=["KEN"],
    )
    product_lower = [p.lower() for p in contract.product_literals]
    assert "production" not in product_lower
    assert any("rice" in p for p in product_lower)
    assert "'production'" not in contract.required_filters_sql.lower()


def test_compile_table_bind_contract_strips_food_security_entity_on_prod() -> None:
    facets = {
        "geography": ["Kenya"],
        "entities": ["food security", "rice"],
        "time_start": "2016-01-01",
        "time_end": "2016-12-31",
        "primary_measures": ["production"],
    }
    contract = compile_table_bind_contract(
        "agg_production_country_year",
        facets=facets,
        query="rice production in kenya 2016",
        country_labels=["KEN"],
    )
    product_lower = {p.lower() for p in contract.product_literals}
    assert "food security" not in product_lower
    assert not any("food security" in p.lower() for p in contract.product_literals)


@pytest.mark.parametrize(
    ("table_id", "query", "entities", "primary_measures", "excluded", "expected_crop"),
    [
        (
            "agg_production_country_year",
            "what is the production of rice in kenya in 2016",
            ["production", "rice"],
            ["production"],
            {"production"},
            "rice",
        ),
        (
            "fct_trade",
            "maize exports from Nigeria in 2020",
            ["exports", "maize"],
            ["trade"],
            {"exports", "export", "trade"},
            "maize",
        ),
        (
            "fct_yield",
            "wheat yield in Ethiopia in 2019",
            ["yield", "wheat"],
            ["yield"],
            {"yield"},
            "wheat",
        ),
        (
            "fct_prices",
            "retail price of maize in Mali in 2024",
            ["price", "maize"],
            ["market_price"],
            {"price", "prices"},
            "maize",
        ),
    ],
)
def test_compile_table_bind_contract_strips_ontology_measure_tokens(
    table_id: str,
    query: str,
    entities: list[str],
    primary_measures: list[str],
    excluded: set[str],
    expected_crop: str,
) -> None:
    """Measure ids/aliases from MEASURES must not become product_name filters."""
    mart_ids = {r["table_id"] for r in list_mart_table_index()}
    if table_id not in mart_ids:
        pytest.skip(f"{table_id} not in mart index")
    facets = {
        "geography": ["Kenya"] if "kenya" in query else ["Nigeria"] if "Nigeria" in query else ["Ethiopia"] if "Ethiopia" in query else ["Mali"],
        "entities": entities,
        "time_start": "2016-01-01" if "2016" in query else "2020-01-01" if "2020" in query else "2019-01-01" if "2019" in query else "2024-01-01",
        "time_end": "2016-12-31" if "2016" in query else "2020-12-31" if "2020" in query else "2019-12-31" if "2019" in query else "2024-12-31",
        "primary_measures": primary_measures,
    }
    country = "KEN" if "kenya" in query else "NGA" if "Nigeria" in query else "ETH" if "Ethiopia" in query else "MLI"
    contract = compile_table_bind_contract(
        table_id,
        facets=facets,
        query=query,
        country_labels=[country],
    )
    product_lower = {p.lower() for p in contract.product_literals}
    for token in excluded:
        assert token not in product_lower
    if contract.product_literals:
        assert any(expected_crop in p.lower() for p in contract.product_literals)


def test_prod_engine_returns_planned_without_sql() -> None:
    engine = ProdEngine()
    result = engine.run_plan(
        "what is the production of rice in kenya in 2016",
        facets={
            "geography": ["Kenya"],
            "entities": ["Rice"],
            "time_start": "2016-01-01",
            "time_end": "2016-12-31",
            "primary_measures": ["production"],
        },
        card=load_schema_card("PROD") or {},
    )
    assert result.status == "planned"
    assert result.sql is None
    assert result.bind_contract is not None
    assert result.query_intents

"""v3: value_map, resolver (bq_targets/qdrant_terms), routing modes, source_basis, ingestion tagging."""
import json, os, sqlite3, subprocess, sys
import pytest
import opentrace_ontology as oo
from opentrace_ontology import _data

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, ".."))
BUILD = os.path.join(PKG, "ontology_build")
ML_ENG = os.path.abspath(os.path.join(PKG, ".."))
MVI = os.path.join(ML_ENG, "ml", "rag", "value_index", "mart_value_index.json")
HAS_BUILD = os.path.exists(os.path.join(BUILD, "check_value_index_coverage.py"))

def ids(ents): return [e.entity_id for e in ents]
def ent(ents, oid): return next(e for e in ents if e.entity_id == oid)

# ---- value_map table
def test_value_map_present_and_complete():
    n = _data.q("SELECT COUNT(*) FROM value_map")[0][0]
    assert n == int(_data.meta()["value_map_rows"]) and n > 5000
    assert _data.q("SELECT COUNT(*) FROM value_map WHERE entity_id IS NULL OR entity_id=''")[0][0] == 0

def test_value_map_country_forms_per_column():
    t = oo.bq_targets("OT:GEO:NGA")
    assert t["agg_production_annual.country_code"] == ["NG"]
    assert t["fct_production.country_iso3"] == ["NGA"]
    assert t["dim_geography.country_name"] == ["Nigeria"]

def test_value_map_country_variants_same_entity():
    t = oo.bq_targets("OT:GEO:TZA")
    assert {"Tanzania", "Tanzania, United Republic of"} <= set(t["dim_geography.country_name"])
    assert "Côte d’Ivoire" in oo.bq_targets("OT:GEO:CIV")["dim_geography.country_name"]
    assert "DRC" in oo.bq_targets("OT:GEO:COD")["dim_geography.country_name"]

def test_namibia_na_not_treated_as_null():
    assert "NA" in oo.bq_targets("OT:GEO:NAM").get("dim_geography.country_iso2", [])

def test_products_map_to_crop_entities():
    assert "Cassava" in oo.bq_targets("OT:CROP:cassava")["dim_product.product_name"]
    assert {"Maize", "Maize (Yellow)"} <= set(oo.bq_targets("OT:CROP:maize")["agg_production_annual.product_name"])
    assert "Fall Armyworm" in oo.bq_targets("OT:DISEASE:fall_armyworm")["dim_pest.pest_name"]

def test_overrides_fix_pea_not_pears():
    assert "Pea" in oo.bq_targets("OT:PRODUCT:peas").get("agg_production_annual.product_name", [])
    assert "Pea" not in oo.bq_targets("OT:PRODUCT:pears").get("agg_production_annual.product_name", [])

def test_dimension_values_mapped_or_minted():
    assert "Yield" in oo.bq_targets("OT:MEASURE:yield")["fct_production.element"]
    r = _data.q("SELECT entity_id FROM value_map WHERE \"column\"='year' LIMIT 1")[0][0]
    assert r.startswith("OT:MV:year:")

def test_dbt_seed_matches_sqlite():
    p = os.path.join(PKG, "seeds", "ontology_value_map.csv")
    if not os.path.exists(p):
        pytest.skip("seed not in this layout")
    assert sum(1 for _ in open(p, encoding="utf-8")) - 1 == _data.q("SELECT COUNT(*) FROM value_map")[0][0]

# ---- resolve
def test_resolve_typo_crop_and_country():
    es = oo.resolve("What is the current state of casssava in Nigeria")
    assert ids(es) == ["OT:CROP:cassava", "OT:GEO:NGA"]
    c = ent(es, "OT:CROP:cassava"); assert c.fuzzy and c.bq_targets["dim_product.product_name"] == ["Cassava"]
    g = ent(es, "OT:GEO:NGA"); assert g.is_africa and g.bq_targets["fct_prices.country_iso3"] == ["NGA"]

def test_resolve_iso_codes_upper_only():
    assert "OT:GEO:NGA" in ids(oo.resolve("maize in NGA"))
    assert "OT:GEO:NGA" in ids(oo.resolve("maize in NG"))
    es = ids(oo.resolve("production in kenya per hectare can be low"))
    assert "OT:GEO:IND" not in es and "OT:GEO:CAN" not in es and "OT:GEO:KEN" in es

def test_resolve_french_country_and_crop():
    es = ids(oo.resolve("prix du manioc au Sénégal"))
    assert "OT:CROP:cassava" in es and "OT:GEO:SEN" in es
    assert "OT:GEO:CIV" in ids(oo.resolve("cacao en Côte d'Ivoire"))

def test_qdrant_terms_multilingual_and_derived():
    t = ent(oo.resolve("cassava"), "OT:CROP:cassava").qdrant_terms
    low = {x.lower() for x in t}
    assert "cassava" in low and ("manioc" in low or "mandioca" in low)
    assert any("flour" in x or "tapioca" in x or "gari" in x for x in low)

def test_payload_ids_include_entity_and_narrower():
    e = ent(oo.resolve("beans in Ethiopia"), "OT:CROP:common_bean")
    assert e.payload_ids[0] == "OT:CROP:common_bean"

def test_narrower_bq_targets():
    # cereal group has no own row in every column but narrower crops do
    e = oo.resolve("cereals")[0]
    assert e.bq_targets or e.bq_targets_narrower

# ---- route
def test_route_bq_plus_qdrant():
    p = oo.plan_query("cassava production in Nigeria")
    modes = {e["entity_id"]: e["mode"] for e in p["entities"]}
    assert modes["OT:CROP:cassava"] == oo.MODE_BQ and p["source_basis"] == "structured" and p["run_bigquery"]
    assert p["acf_ceiling"] is None and p["african_countries"] == ["NGA"]

def test_route_qdrant_only_when_not_in_marts():
    p = oo.plan_query("mushroom production in Ghana")
    sub = [e for e in p["entities"] if e["entity_type"] != "country" and e["mode"] == oo.MODE_QD]
    assert sub and p["source_basis"] == "documents_only" and p["acf_ceiling"] == "moderate"
    assert not p["run_bigquery"] and p["run_qdrant"]
    assert sub[0]["qdrant_terms"] and sub[0]["payload_ids"]

def test_route_web_fallback_for_unknown():
    p = oo.plan_query("zorblax yields in Togo")
    assert p["web_fallback_terms"] == ["zorblax"] and p["source_basis"] == "web_only"

def test_route_country_alone_is_not_structured_subject():
    p = oo.route(oo.resolve("Nigeria"))
    assert p["source_basis"] == "structured" and p["countries"] == ["NGA"]   # BQ has the country (e.g. country-level facts)

def test_route_empty():
    p = oo.route([])
    assert p["source_basis"] == "none" and not p["run_qdrant"] and p["acf_ceiling"] == "low"

def test_route_bq_targets_from_narrower_flag():
    p = oo.route(oo.resolve("cereals"))
    e = p["entities"][0]
    assert e["bq_targets_from"] in ("self", "narrower")

# ---- profile country cleaning + ingestion tags
@pytest.mark.parametrize("v,exp", [("NG", "NGA"), ("NGA", "NGA"), ("Nigeria", "NGA"), ("nigéria", "NGA"),
                                   ("Albania", None), ("US", None), ("", None), (None, None), ("Côte d’Ivoire", "CIV")])
def test_clean_profile_country(v, exp):
    assert oo.clean_profile_country(v) == exp

def test_clean_profile_country_non_africa_opt_in():
    assert oo.clean_profile_country("Albania", africa_only=False) == "ALB"

def test_tag_chunk():
    t = oo.tag_chunk("Cassava mosaic disease and fall armyworm hit maize farms in Uganda and Kenya.")
    assert "OT:DISEASE:cassava_mosaic" in t["ontology_ids"] and "OT:CROP:maize" in t["ontology_ids"]
    assert "OT:GROUP:cereals" in t["ontology_ids"]          # broader ancestor for group filters
    assert t["ontology_countries"] == ["UGA", "KEN"]
    assert all(not i.startswith("OT:MV:") for i in t["ontology_ids"])

def test_resolve_is_fast_warm():
    import time
    oo.plan_query("maize price in Kenya")
    t = time.perf_counter()
    for _ in range(200): oo.plan_query("What is the current state of casssava in Nigeria")
    assert (time.perf_counter() - t) / 200 < 0.02

# ---- CI coverage gate
@pytest.mark.skipif(not HAS_BUILD, reason="ontology_build not available")
def test_coverage_check_passes_and_fails_without_allowlist(tmp_path):
    s = os.path.join(BUILD, "check_value_index_coverage.py")
    ok = subprocess.run(
        [sys.executable, s, "--mvi", MVI, "--out", str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert ok.returncode == 0, ok.stderr
    assert (tmp_path / "value_index_coverage.csv").exists() and (
        tmp_path / "value_index_coverage.md"
    ).exists()
    empty = tmp_path / "allow.csv"
    empty.write_text("column_key,stored_value,reason\n")
    bad = subprocess.run(
        [sys.executable, s, "--mvi", MVI, "--out", str(tmp_path), "--allowlist", str(empty)],
        capture_output=True,
        text=True,
    )
    assert bad.returncode == 1 and "UNMAPPED" in bad.stderr


@pytest.mark.skipif(not HAS_BUILD, reason="ontology_build not available")
def test_coverage_check_detects_stale_value_index(tmp_path):
    src = json.load(open(MVI, encoding="utf-8"))
    src["enums"]["dim_product.product_name"].append("Dragonfruit")
    p = tmp_path / "mvi.json"
    p.write_text(json.dumps(src))
    r = subprocess.run(
        [
            sys.executable,
            os.path.join(BUILD, "check_value_index_coverage.py"),
            "--mvi",
            str(p),
            "--out",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 1 and "another value index" in r.stderr and "Dragonfruit" in r.stderr

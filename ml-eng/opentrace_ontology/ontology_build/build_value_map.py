#!/usr/bin/env python3
"""v3: map every mart_value_index.json enum value (table.column -> stored value) to an ontology entity ID.
Post-processes opentrace_ontology/data/ontology.sqlite (run after build_sqlite.py). Offline, deterministic.
Adds tables:
  value_map(entity_id, "table", "column", stored_value, match_type, col_class)   one row per enum value
  geo(entity_id, iso2, iso3, name, is_africa)                                       OT:GEO:<ISO3>
  aux_aliases(alias_norm, entity_id, kind)                                          country forms + mart dimension labels
  mart_values(entity_id, kind, label)                                               minted OT:MV:* ids for warehouse-only values
Usage: build_value_map.py [--mvi PATH] [--data DIR] [--dbt-seed-out DIR]
"""
import argparse, csv, json, os, re, sqlite3, sys, hashlib, unicodedata
HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, ".."))
ML_ENG = os.path.abspath(os.path.join(PKG, ".."))
def _env(n, d): return os.environ.get(n, d).strip() or d
ap = argparse.ArgumentParser()
ap.add_argument("--mvi", default=_env("ONTOLOGY_VALUE_INDEX", os.path.join(ML_ENG, "ml", "rag", "value_index", "mart_value_index.json")))
ap.add_argument("--data", default=_env("ONTOLOGY_DATA_DIR", os.path.join(PKG, "data")))
ap.add_argument("--seeds", default=_env("ONTOLOGY_SEEDS_DIR", os.path.join(PKG, "seeds")))
ap.add_argument("--dbt-seed-out", default=None, help="Optional dir to also write ontology_value_map.csv")
ap.add_argument("--overrides", default=os.path.join(HERE, "curated", "value_map_overrides.csv"))
ap.add_argument("--iso3166", default=os.path.join(HERE, "inputs", "iso3166.csv"))
_args, _unknown = ap.parse_known_args()
if ML_ENG not in sys.path: sys.path.insert(0, ML_ENG)
from opentrace_ontology._text import key_of, fold
DB = os.path.join(os.path.abspath(_args.data), "ontology.sqlite")
MVI = os.path.abspath(_args.mvi)
OVR = os.path.abspath(_args.overrides)
ISO3166 = os.path.abspath(_args.iso3166)
SEEDS_DIR = os.path.abspath(_args.seeds)
DBT_SEED_OUT = os.path.abspath(_args.dbt_seed_out) if _args.dbt_seed_out else None

AFRICA = set("""DZA AGO BEN BWA BFA BDI CPV CMR CAF TCD COM COD COG CIV DJI EGY GNQ ERI SWZ ETH GAB GMB GHA GIN GNB KEN LSO LBR
LBY MDG MWI MLI MRT MUS MAR MOZ NAM NER NGA RWA STP SEN SYC SLE SOM ZAF SSD SDN TZA TGO TUN UGA ZMB ZWE ESH MYT REU SHN IOT ATF""".split())
# extra country spellings seen in marts / common usage (folded keys)
EXTRA_GEO = {"drc": "COD", "congo (kinshasa)": "COD", "congo kinshasa": "COD", "democratic republic of the congo": "COD",
    "congo (brazzaville)": "COG", "congo brazzaville": "COG", "republic of the congo": "COG", "tanzania": "TZA",
    "tanzania, united republic of": "TZA", "united republic of tanzania": "TZA", "gambia, the": "GMB", "the gambia": "GMB",
    "gambia": "GMB", "cote d'ivoire": "CIV", "ivory coast": "CIV", "cote divoire": "CIV", "cabo verde": "CPV", "cape verde": "CPV",
    "eswatini": "SWZ", "swaziland": "SWZ", "reunion": "REU", "sao tome and principe": "STP", "libya": "LBY", "egypt": "EGY",
    "western sahara": "ESH", "ascension, saint helena and tristan da cunha": "SHN", "united states": "USA", "usa": "USA",
    "united kingdom": "GBR", "uk": "GBR", "china (people's republic of)": "CHN", "russia": "RUS", "iran (islamic republic of)": "IRN",
    "korea (republic of)": "KOR", "south korea": "KOR", "north korea": "PRK", "former yugoslav republic of macedonia": "MKD",
    "turkey": "TUR", "czech republic": "CZE", "vietnam": "VNM", "viet nam": "VNM", "bolivia": "BOL", "venezuela": "VEN",
    "syria": "SYR", "laos": "LAO", "moldova": "MDA", "taiwan": "TWN", "kosovo": "XKX", "netherlands antilles": "ANT",
    "yugoslavia": "YUG", "european union": "EUU", "hong kong": "HKG", "macao": "MAC", "macau": "MAC", "palestine": "PSE",
    "micronesia": "FSM", "moldova (republic of)": "MDA", "kosovo * un resolution": "XKX", "tanzania (united republic of)": "TZA",
    "palestinian-administered areas": "PSE", "congo (democratic republic of)": "COD", "libyan arab jamahiriya": "LBY",
    "lao (people's democratic republic)": "LAO", "virgin islands (british)": "VGB", "sudan (former)": "SDN", "ethiopia pdr": "ETH", "brunei": "BRN", "nigerie": "NGA", "nigéria": "NGA", "senegal": "SEN", "cameroun": "CMR",
    "maroc": "MAR", "algerie": "DZA", "tunisie": "TUN", "egypte": "EGY", "ethiopie": "ETH", "afrique du sud": "ZAF",
    "mocambique": "MOZ", "angola": "AGO", "guine-bissau": "GNB", "cabo-verde": "CPV", "quenia": "KEN", "africa do sul": "ZAF"}
EXTRA_CODES = {"XK": "XKX", "EU": "EUU", "AN": "ANT", "YU": "YUG"}
NULLISH = {"unknown", "none", "zz", "null", "n/a", ""}

def col_class(table, col):
    if re.search(r"country_(iso2|iso3|code|name)$", col): return "country"
    if col in ("product_name", "variety_name", "pest_name", "season_name"): return "entity"
    if col in ("year", "month", "month_code", "month_name") or col.endswith("_grain") or col in (
        "source_name", "price_source", "status_code", "status_type", "sensor_type", "currency_code", "unit_type",
        "measure_type", "trade_type", "housing_type", "breed_type", "village_name", "primary_funder_name",
        "organisation_name", "phase_code", "scenario_code", "system_code", "category_code", "element_code"): return "structural"
    return "dimension"   # metric, element, indicator, unit, land use, systems, categories, ...

def slug(s): return re.sub(r"[^a-z0-9]+", "_", fold(s)).strip("_")[:60] or hashlib.md5(s.encode()).hexdigest()[:8]
def humanize(v): return re.sub(r"\s+", " ", v.replace("_", " ")).strip()

def main():
    db = sqlite3.connect(DB); c = db.cursor()
    mvi = json.load(open(MVI, encoding="utf-8")); enums = mvi.get("enums", {})
    for t in ("value_map", "geo", "aux_aliases", "mart_values"): c.execute(f"DROP TABLE IF EXISTS {t}")
    c.executescript("""CREATE TABLE value_map(entity_id TEXT, "table" TEXT, "column" TEXT, stored_value TEXT, match_type TEXT, col_class TEXT);
    CREATE TABLE geo(entity_id TEXT PRIMARY KEY, iso2 TEXT, iso3 TEXT, name TEXT, is_africa INTEGER) WITHOUT ROWID;
    CREATE TABLE aux_aliases(alias_norm TEXT, entity_id TEXT, kind TEXT, PRIMARY KEY(alias_norm, entity_id)) WITHOUT ROWID;
    CREATE TABLE mart_values(entity_id TEXT PRIMARY KEY, kind TEXT, label TEXT) WITHOUT ROWID;""")
    # ---- geo
    iso = list(csv.DictReader(open(ISO3166, encoding="utf-8")))
    by2 = {r["iso2"]: r["iso3"] for r in iso}; by3 = {r["iso3"]: r for r in iso}
    by2.update(EXTRA_CODES)
    names = {}
    for r in iso:
        for n in (r["name"], r["official_name"], r["common_name"]):
            if n: names[key_of(n)] = r["iso3"]
    for k, v in EXTRA_GEO.items(): names[key_of(k)] = v
    for code, nm in (("XKX", "Kosovo"), ("EUU", "European Union"), ("ANT", "Netherlands Antilles"), ("YUG", "Yugoslavia")):
        by3.setdefault(code, {"iso2": "", "iso3": code, "name": nm})
    geo_rows = {}
    def geo(iso3):
        oid = f"OT:GEO:{iso3}"
        if oid not in geo_rows:
            r = by3[iso3]; geo_rows[oid] = (oid, r["iso2"] or {v: k for k, v in by2.items()}.get(iso3, ""), iso3, r["name"], int(iso3 in AFRICA))
        return oid
    def resolve_country(v):
        s = v.strip().replace("\u2019", "'")
        if fold(s) in NULLISH: return None, None
        if len(s) == 2 and s.upper() in by2: return geo(by2[s.upper()]), "iso2"
        if len(s) == 3 and s.upper() in by3: return geo(s.upper()), "iso3"
        k = key_of(s)
        if k in names: return geo(names[k]), "country_name"
        return None, None
    aux = set()
    for r in iso:
        if r["iso3"] in AFRICA:  # every African country gets its names as aliases even if not yet in a mart
            oid = geo(r["iso3"])
            for n in (r["name"], r["official_name"], r["common_name"]):
                if n: aux.add((key_of(n), oid, "country"))
    for k, v in EXTRA_GEO.items():
        if v in by3: aux.add((key_of(k), geo(v), "country"))
    # ---- ontology lookup helpers
    E = {i: (t, n) for i, t, n in c.execute("SELECT id,type,name FROM entities")}
    eid2id = {e: i for e, i in c.execute("SELECT eid,id FROM entities")}
    PREF = {"crop": 0, "product": 1, "commodity": 2, "crop_group": 2, "disease_pest": 0, "season": 0, "animal": 1, "variety": 0,
            "breed": 0, "measure": 3, "indicator_class": 3, "unit": 3, "by_product": 2, "input": 3, "organism": 4, "process": 5}
    ATW = {"preferred": 0, "synonym": 1, "plural": 1, "local": 2, "scientific": 1, "abbrev": 3, "misspelling": 4}
    def exact(k, want=None):
        best = None
        for eid, at, src in c.execute("SELECT eid,atype,src FROM aliases WHERE alias_norm=? AND atype!='misspelling'", (k,)):
            oid = eid2id[eid]; t = E[oid][0]
            if want and (t not in want or at == "abbrev"): continue
            sc = (PREF.get(t, 6), ATW.get(at, 5), 0 if src.startswith("opentrace") else 1 if src == "faostat" else 2, oid)
            if best is None or sc < best[0]: best = (sc, oid)
        return best[1] if best else None
    singular = lambda k: re.sub(r"(oes|ies|s)$", lambda m: {"oes": "o", "ies": "y", "s": ""}[m.group(1)], k)
    ovr = {}
    if os.path.exists(OVR):
        for r in csv.DictReader(open(OVR, encoding="utf-8")):
            if not (r["entity_id"] in E or r["entity_id"].startswith(("OT:GEO:", "OT:MV:"))): sys.exit(f"override -> unknown id {r}")
            for ck in ([r["column_key"]] if r["column_key"] != "*" else [k for k in enums if k.endswith("." + r.get("column", ""))] or list(enums)):
                ovr[(ck, r["stored_value"])] = r["entity_id"]
    def map_entity(v, cls):
        k = key_of(v)
        want = None if cls == "entity" else {"measure", "indicator_class", "unit", "process", "farming_system", "input", "service",
                                                 "value_chain_stage", "commodity_aggregate", "season", "equipment"}
        for cand, mt in ((k, "exact_alias"), (singular(k), "singular"), (k + "s", "plural"), (k + "es", "plural"),
                         (k + " season" if cls == "entity" else "", "season_suffix"),
                         (key_of(re.sub(r"\s*\(.*?\)", "", v)), "base_term"), (key_of(humanize(v)), "humanized")):
            if cand and (o := exact(cand, want)): return o, mt
        if cls == "entity":  # "Sorghum (Red)" -> sorghum ; "Cotton (American)" ; "Maize (Yellow)"
            toks = key_of(re.sub(r"[()&,/]", " ", v)).split()
            for n in range(len(toks), 0, -1):
                for s in range(0, len(toks) - n + 1):
                    sub = " ".join(toks[s:s + n])
                    if len(sub) > 3 and (o := exact(sub) or exact(singular(sub))): return o, "partial_term"
        return None, None
    # ---- map every enum value
    vm, minted = [], {}
    for ck in sorted(enums):
        table, col = ck.split(".", 1); cls = col_class(table, col)
        for v in enums[ck]:
            v = str(v); oid = mt = None
            if (ck, v) in ovr: oid, mt = ovr[(ck, v)], "override"
            elif cls == "country": oid, mt = resolve_country(v)
            elif cls in ("entity", "dimension"): oid, mt = map_entity(v, cls)
            if oid is None:
                kind = cls if cls != "country" else "country_unresolved"
                oid = f"OT:MV:{col}:{slug(v)}"; mt = "minted_" + cls
                minted[oid] = (oid, kind, humanize(v) if cls != "structural" else v)
            if cls == "country" and mt in ("iso2", "iso3", "country_name"): aux.add((key_of(v), oid, "country_form"))
            if cls in ("entity", "dimension") and len(key_of(v)) > 2 and fold(v) not in NULLISH:
                aux.add((key_of(humanize(v)), oid, "mart_" + cls))
            vm.append((oid, table, col, v, mt, cls))
    c.executemany("INSERT INTO value_map VALUES(?,?,?,?,?,?)", vm)
    c.executemany("INSERT INTO geo VALUES(?,?,?,?,?)", sorted(geo_rows.values()))
    c.executemany("INSERT OR IGNORE INTO aux_aliases VALUES(?,?,?)", sorted(a for a in aux if a[0]))
    c.executemany("INSERT INTO mart_values VALUES(?,?,?)", sorted(minted.values()))
    c.executescript('CREATE INDEX vm_e ON value_map(entity_id); CREATE INDEX vm_tc ON value_map("table","column");')
    h = hashlib.sha256(open(MVI, "rb").read()).hexdigest()
    c.execute("INSERT OR REPLACE INTO meta VALUES('value_index_sha256',?)", (h,))
    c.execute("INSERT OR REPLACE INTO meta VALUES('value_map_rows',?)", (str(len(vm)),))
    db.commit(); c.execute("VACUUM"); db.close()
    out_rows = sorted(vm, key=lambda r: (r[1], r[2], r[3]))
    header = ["entity_id", "table_name", "column_name", "stored_value", "match_type", "col_class"]
    targets = [os.path.join(SEEDS_DIR, "ontology_value_map.csv")]
    if DBT_SEED_OUT:
        targets.append(os.path.join(DBT_SEED_OUT, "ontology_value_map.csv"))
    for path in targets:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(header); w.writerows(out_rows)
    print(f"value_map rows={len(vm)} geo={len(geo_rows)} aux_aliases={len(aux)} minted={len(minted)}")
if __name__ == "__main__": main()

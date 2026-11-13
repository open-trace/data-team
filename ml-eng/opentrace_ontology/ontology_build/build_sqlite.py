#!/usr/bin/env python3
"""CSV seeds (source of truth) -> opentrace_ontology/data/{ontology.sqlite, matcher.*}.
Deterministic: same CSVs -> same row content; seeds_sha256 stored in table meta for CI check.
"""
from __future__ import annotations

import argparse
import collections
import csv
import gzip
import hashlib
import json
import os
import re
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, ".."))
ML_ENG = os.path.abspath(os.path.join(PKG, ".."))


def _env_or(name: str, default: str) -> str:
    return os.environ.get(name, default).strip() or default


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--seeds",
        default=_env_or("ONTOLOGY_SEEDS_DIR", os.path.join(PKG, "seeds")),
        help="Directory with ontology_*.csv seeds (default: ../seeds or ONTOLOGY_SEEDS_DIR)",
    )
    p.add_argument(
        "--data",
        default=_env_or("ONTOLOGY_DATA_DIR", os.path.join(PKG, "data")),
        help="Output data directory (default: ../data or ONTOLOGY_DATA_DIR)",
    )
    p.add_argument(
        "--faostat-areas",
        default=os.path.join(HERE, "inputs", "faostat_area_codes.csv"),
        help="FAOSTAT area codes CSV for known_words enrichment",
    )
    return p.parse_args()


SEEDS = [
    "ontology_entities",
    "ontology_aliases",
    "ontology_relations",
    "ontology_codes",
    "ontology_warehouse_map",
]
W = {
    "preferred": 1.0,
    "scientific": 0.97,
    "synonym": 0.95,
    "local": 0.95,
    "plural": 0.95,
    "abbrev": 0.8,
    "misspelling": 0.85,
}
CORE = {
    "crop",
    "animal",
    "product",
    "by_product",
    "input",
    "disease_pest",
    "commodity",
    "crop_group",
    "animal_group",
    "variety",
    "breed",
    "service",
}
CLOSURE_RELS = ("broader", "derived_from")


def main() -> None:
    args = _parse_args()
    seeds_dir = os.path.abspath(args.seeds)
    data_dir = os.path.abspath(args.data)
    os.makedirs(data_dir, exist_ok=True)

    if ML_ENG not in sys.path:
        sys.path.insert(0, ML_ENG)
    from opentrace_ontology._text import STOPWORDS, fold, key_of
    from opentrace_ontology.ontology_build.seed_io import (
        open_seed,
        resolve_seed_path,
        seed_file_bytes_for_hash,
    )

    def seeds_hash() -> str:
        h = hashlib.sha256()
        for n in SEEDS:
            h.update(n.encode())
            h.update(seed_file_bytes_for_hash(resolve_seed_path(seeds_dir, n)))
        return h.hexdigest()

    def rows(n: str) -> list[dict[str, str]]:
        with open_seed(resolve_seed_path(seeds_dir, n)) as f:
            return list(csv.DictReader(f))

    p = os.path.join(data_dir, "ontology.sqlite")
    if os.path.exists(p):
        os.remove(p)
    db = sqlite3.connect(p)
    c = db.cursor()
    c.executescript(
        """PRAGMA page_size=4096; PRAGMA journal_mode=OFF;
    CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;
    CREATE TABLE entities(eid INTEGER PRIMARY KEY, id TEXT UNIQUE, type TEXT, name TEXT, sci TEXT, parent TEXT, attrs TEXT, src TEXT);
    CREATE TABLE aliases(rid INTEGER PRIMARY KEY, eid INTEGER, alias TEXT, alias_norm TEXT, lang TEXT, country TEXT, atype TEXT, src TEXT, weight REAL, review_status TEXT);
    CREATE TABLE codes(eid INTEGER, scheme TEXT, code TEXT, src TEXT);
    CREATE TABLE warehouse_map(eid INTEGER, bq_table TEXT, col TEXT, value TEXT, match_type TEXT, provenance TEXT);
    CREATE TABLE relations(src INTEGER, rel TEXT, dst INTEGER, source TEXT, PRIMARY KEY(src,rel,dst)) WITHOUT ROWID;
    CREATE TABLE closure(ancestor INTEGER, rel TEXT, descendant INTEGER, depth INTEGER, PRIMARY KEY(ancestor,rel,descendant)) WITHOUT ROWID;
    CREATE TABLE fuzzy_vocab(w TEXT PRIMARY KEY) WITHOUT ROWID;
    CREATE TABLE known_words(w TEXT PRIMARY KEY) WITHOUT ROWID;"""
    )
    E = rows("ontology_entities")
    ents = {e["ontology_id"]: e for e in E}
    EID = {e["ontology_id"]: n for n, e in enumerate(sorted(E, key=lambda e: e["ontology_id"]), 1)}
    c.executemany(
        "INSERT INTO entities VALUES(?,?,?,?,?,?,?,?)",
        [
            (
                EID[e["ontology_id"]],
                e["ontology_id"],
                e["entity_type"],
                e["canonical_name_en"],
                e["scientific_name"],
                e["parent_id"],
                e["attributes"],
                e["primary_source"],
            )
            for e in E
        ],
    )
    A = rows("ontology_aliases")
    arows: list[tuple] = []
    vocab: set[str] = set()
    known: set[str] = set()
    keys: collections.defaultdict[str, set[str]] = collections.defaultdict(set)
    for r in A:
        k = key_of(r["alias"])
        if not k:
            continue
        arows.append(
            (
                EID[r["ontology_id"]],
                None if r["alias"] == k else r["alias"],
                k,
                r["language"],
                r["country_iso3"],
                r["alias_type"],
                r["source"],
                W.get(r["alias_type"], 0.9),
                (r.get("review_status") or "").strip(),
            )
        )
        if not (len(k) <= 3 and r["source"] != "opentrace_curated" and r["alias_type"] != "abbrev"):
            keys[k].add(r["ontology_id"])
        e = ents[r["ontology_id"]]
        if r["alias_type"] != "misspelling":
            if r["language"] == "eng":
                known |= set(re.findall(r"[a-z]{5,}", k))
            if (
                (r["language"] in ("eng", "fra", "por", "lat") or r["source"] == "opentrace_curated")
                and e["entity_type"] in CORE
                and '"taxon": true' not in e["attributes"]
                and " " not in k
                and len(k) >= 5
                and k.isalpha()
                and k not in STOPWORDS
            ):
                vocab.add(k)
    c.executemany(
        "INSERT INTO aliases(eid,alias,alias_norm,lang,country,atype,src,weight,review_status) VALUES(?,?,?,?,?,?,?,?,?)",
        arows,
    )
    known -= vocab
    faostat = args.faostat_areas
    if os.path.exists(faostat):
        for r in csv.DictReader(open(faostat, encoding="utf-8-sig")):
            known |= set(re.findall(r"[a-z]{5,}", fold(r["Area"])))
    known |= {
        "tanzania",
        "congo",
        "ivoire",
        "burkina",
        "eswatini",
        "zimbabwe",
        "mozambique",
        "cameroon",
        "nigeria",
        "kenya",
    }
    c.executemany("INSERT INTO fuzzy_vocab VALUES(?)", [(w,) for w in sorted(vocab)])
    c.executemany("INSERT INTO known_words VALUES(?)", [(w,) for w in sorted(known)])
    c.executemany(
        "INSERT INTO codes VALUES(?,?,?,?)",
        [(EID[r["ontology_id"]], r["scheme"], r["code"], r["source"]) for r in rows("ontology_codes")],
    )
    c.executemany(
        "INSERT INTO warehouse_map VALUES(?,?,?,?,?,?)",
        [(EID[r["ontology_id"]],) + tuple(list(r.values())[1:]) for r in rows("ontology_warehouse_map")],
    )
    Rl = rows("ontology_relations")
    c.executemany(
        "INSERT OR IGNORE INTO relations VALUES(?,?,?,?)",
        [
            (EID[r["subject_id"]], r["predicate"], EID[r["object_id"]], r["source"])
            for r in Rl
        ],
    )
    up: collections.defaultdict = collections.defaultdict(lambda: collections.defaultdict(set))
    for r in Rl:
        if r["predicate"] in CLOSURE_RELS:
            up[r["predicate"]][r["subject_id"]].add(r["object_id"])
    cl: list[tuple] = []
    for rel, g in up.items():
        for s in g:
            seen = {s: 0}
            frontier = [s]
            d = 0
            while frontier and d < 50:
                d += 1
                nxt = []
                for x in frontier:
                    for y in g.get(x, ()):
                        if y not in seen:
                            seen[y] = d
                            nxt.append(y)
                frontier = nxt
            cl += [(EID[a], rel, EID[s], dd) for a, dd in seen.items() if dd > 0]
    c.executemany("INSERT INTO closure VALUES(?,?,?,?)", cl)
    c.executescript(
        """
    CREATE INDEX ix_alias_norm ON aliases(alias_norm, lang, country);
    CREATE INDEX ix_alias_eid ON aliases(eid);
    CREATE INDEX ix_codes_id ON codes(eid);
    CREATE INDEX ix_codes_code ON codes(code);
    CREATE INDEX ix_wh_id ON warehouse_map(eid);
    CREATE INDEX ix_rel_dst ON relations(dst, rel);
    CREATE INDEX ix_cl_desc ON closure(descendant, rel);
    CREATE VIRTUAL TABLE aliases_fts USING fts5(alias_norm, content='aliases', content_rowid='rid', columnsize=0, detail=column, tokenize='unicode61 remove_diacritics 2');
    INSERT INTO aliases_fts(rowid, alias_norm) SELECT rid, alias_norm FROM aliases WHERE src<>'generated';
    INSERT INTO aliases_fts(aliases_fts) VALUES('optimize');"""
    )
    h = seeds_hash()
    c.executemany(
        "INSERT INTO meta VALUES(?,?)",
        [
            ("seeds_sha256", h),
            ("n_entities", str(len(E))),
            ("n_aliases", str(len(arows))),
            ("n_closure", str(len(cl))),
            ("schema_version", "2"),
        ],
    )
    db.commit()
    c.execute("ANALYZE")
    db.commit()
    db.execute("VACUUM")
    db.close()
    ks = sorted(keys)
    with gzip.GzipFile(
        os.path.join(data_dir, "matcher_keys.txt.gz"), "wb", mtime=0, compresslevel=9
    ) as f:
        f.write("\n".join(ks).encode())
    try:
        import ahocorasick  # noqa: F401

        print(
            "pyahocorasick available at build; automaton is rebuilt lazily at runtime "
            "from matcher_keys.txt.gz"
        )
    except ImportError:
        pass
    json.dump(
        {"seeds_sha256": h, "matcher_keys": len(ks)},
        open(os.path.join(data_dir, "build_info.json"), "w"),
    )
    print(
        json.dumps(
            {
                "entities": len(E),
                "aliases": len(arows),
                "closure": len(cl),
                "matcher_keys": len(ks),
                "vocab": len(vocab),
            }
        )
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""CI guard: fail if ontology.sqlite was not rebuilt from the current seed CSVs.
Usage: python ontology_build/check_sqlite_matches_csv.py [--seeds DIR] [--db PATH]
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, ".."))
ML_ENG = os.path.abspath(os.path.join(PKG, ".."))


def _env(n: str, d: str) -> str:
    return os.environ.get(n, d).strip() or d


if ML_ENG not in sys.path:
    sys.path.insert(0, ML_ENG)
from opentrace_ontology.ontology_build.seed_io import (  # noqa: E402
    open_seed,
    resolve_seed_path,
    seed_file_bytes_for_hash,
)

ap = argparse.ArgumentParser()
ap.add_argument("--seeds", default=_env("ONTOLOGY_SEEDS_DIR", os.path.join(PKG, "seeds")))
ap.add_argument(
    "--db",
    default=os.path.join(_env("ONTOLOGY_DATA_DIR", os.path.join(PKG, "data")), "ontology.sqlite"),
)
a = ap.parse_args()
SEEDS = [
    "ontology_entities",
    "ontology_aliases",
    "ontology_relations",
    "ontology_codes",
    "ontology_warehouse_map",
]
h = hashlib.sha256()
for n in SEEDS:
    h.update(n.encode())
    h.update(seed_file_bytes_for_hash(resolve_seed_path(a.seeds, n)))
db = sqlite3.connect(f"file:{a.db}?mode=ro&immutable=1", uri=True)
meta = dict(db.execute("select k,v from meta"))
err: list[str] = []
if meta.get("seeds_sha256") != h.hexdigest():
    err.append(
        f"seed hash mismatch: csv={h.hexdigest()[:12]} sqlite={meta.get('seeds_sha256', '?')[:12]}"
    )


def cnt(n: str) -> int:
    with open_seed(resolve_seed_path(a.seeds, n)) as f:
        return sum(1 for _ in csv.reader(f)) - 1


for n, t in (
    ("ontology_entities", "entities"),
    ("ontology_codes", "codes"),
    ("ontology_warehouse_map", "warehouse_map"),
):
    c1, c2 = cnt(n), db.execute(f"select count(*) from {t}").fetchone()[0]
    if c1 != c2:
        err.append(f"{t}: csv {c1} != sqlite {c2}")
kf = os.path.join(os.path.dirname(a.db), "matcher_keys.txt.gz")
nk = len([k for k in gzip.open(kf, "rt", encoding="utf-8").read().split("\n") if k])
nd = db.execute("select count(distinct alias_norm) from aliases").fetchone()[0]
if nk > nd or nk == 0:
    err.append(f"matcher keys {nk} inconsistent with {nd} distinct aliases")
print("\n".join(err) or f"OK sqlite matches seeds ({h.hexdigest()[:12]})")
sys.exit(1 if err else 0)

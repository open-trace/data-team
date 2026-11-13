#!/usr/bin/env python3
"""Compare SHA256 of ml-eng package seeds vs data-eng/dbt/seeds/ontology.

ml-eng may store ontology_aliases as .csv.gz; dbt always uses plain .csv.
--check compares decompressed content hashes.
--write streams ml-eng seeds to plain dbt CSVs (never leaves plain aliases in ml-eng).
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, ".."))
ML_ENG = os.path.abspath(os.path.join(PKG, ".."))
REPO = os.path.abspath(os.path.join(ML_ENG, ".."))

if ML_ENG not in sys.path:
    sys.path.insert(0, ML_ENG)
from opentrace_ontology.ontology_build.seed_io import (  # noqa: E402
    gunzip_to_path,
    resolve_seed_path,
    sha256_seed_content,
)

CSV_NAMES = [
    "ontology_entities.csv",
    "ontology_aliases.csv",
    "ontology_relations.csv",
    "ontology_codes.csv",
    "ontology_warehouse_map.csv",
    "ontology_value_map.csv",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--src", default=os.path.join(PKG, "seeds"))
    ap.add_argument("--dst", default=os.path.join(REPO, "data-eng", "dbt", "seeds", "ontology"))
    args = ap.parse_args()
    if not args.check and not args.write:
        print("pass --check or --write", file=sys.stderr)
        return 2
    src, dst = args.src, args.dst
    os.makedirs(dst, exist_ok=True)
    if args.write:
        for name in CSV_NAMES:
            sp = resolve_seed_path(src, name)
            if not os.path.exists(sp):
                print(f"missing src {sp}", file=sys.stderr)
                continue
            dp = os.path.join(dst, name)  # always plain .csv in dbt
            if sp.endswith(".gz"):
                gunzip_to_path(sp, dp)
            else:
                shutil.copy2(sp, dp)
        schema = os.path.join(dst, "schema.yml")
        src_schema = os.path.join(REPO, "opentrace_ontology", "dbt_seeds", "schema.yml")
        if not os.path.exists(schema) and os.path.exists(src_schema):
            shutil.copy2(src_schema, schema)
        # Never leave plain aliases in ml-eng when gz is the package form
        plain_aliases = os.path.join(src, "ontology_aliases.csv")
        gz_aliases = os.path.join(src, "ontology_aliases.csv.gz")
        if os.path.isfile(gz_aliases) and os.path.isfile(plain_aliases):
            os.remove(plain_aliases)
        print(f"wrote seeds -> {dst}")
        return 0
    drift = []
    for name in CSV_NAMES:
        sp = resolve_seed_path(src, name)
        dp = os.path.join(dst, name)
        if not os.path.exists(sp):
            drift.append(f"missing src {sp}")
            continue
        if not os.path.exists(dp):
            drift.append(f"missing dst {dp}")
            continue
        if sha256_seed_content(sp) != sha256_seed_content(dp):
            drift.append(f"hash mismatch {name}")
    if drift:
        print("\n".join(drift), file=sys.stderr)
        return 1
    print(f"OK seeds in sync ({len(CSV_NAMES)} csv)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Demote non-lat preferred aliases that are scientific Latin to alias_type=scientific.

Never touches source=opentrace_curated. Keeps original language.
Drops a row only when an exact duplicate scientific row already exists
(same ontology_id, fold(alias), language).
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import unicodedata

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, ".."))
ML_ENG = os.path.abspath(os.path.join(PKG, ".."))
SEEDS = os.path.join(PKG, "seeds")

if ML_ENG not in sys.path:
    sys.path.insert(0, ML_ENG)
from opentrace_ontology.ontology_build.seed_io import (  # noqa: E402
    open_seed,
    resolve_seed_path,
    write_seed_rows,
)

LAT_RE = re.compile(r"^[A-Z][a-z]+ [a-z]+(?: (?:subsp|var)\. [a-z]+)?$")


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKC", (s or "").lower().strip())
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return " ".join(s.split())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", default=SEEDS)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    ent_path = resolve_seed_path(args.seeds, "ontology_entities")
    alias_path = resolve_seed_path(args.seeds, "ontology_aliases")
    sci_by_id: dict[str, str] = {}
    with open_seed(ent_path) as f:
        for row in csv.DictReader(f):
            sci_by_id[row["ontology_id"]] = fold(row.get("scientific_name") or "")

    with open_seed(alias_path) as f:
        reader = csv.DictReader(f)
        fields = list(reader.fieldnames or [])
        rows = list(reader)

    # Exact scientific keys: (ontology_id, fold(alias), language)
    sci_keys: set[tuple[str, str, str]] = set()
    for row in rows:
        if row.get("alias_type") == "scientific":
            sci_keys.add(
                (
                    row["ontology_id"],
                    fold(row["alias"]),
                    row.get("language") or "",
                )
            )

    demoted = dropped = 0
    out: list[dict] = []
    for row in rows:
        if (
            row.get("alias_type") == "preferred"
            and row.get("source") != "opentrace_curated"
            and (row.get("language") or "") not in ("eng", "lat", "")
        ):
            alias = (row.get("alias") or "").strip()
            folded = fold(alias)
            lang = row.get("language") or ""
            sci = sci_by_id.get(row["ontology_id"], "")
            is_latin = bool(LAT_RE.match(alias)) or (sci and folded == sci)
            if is_latin:
                key = (row["ontology_id"], folded, lang)
                if key in sci_keys:
                    dropped += 1
                    continue
                row = dict(row)
                row["alias_type"] = "scientific"
                # keep original language
                sci_keys.add(key)
                demoted += 1
        out.append(row)

    print(f"demoted={demoted} dropped_exact_dupes={dropped} total_out={len(out)}")
    if args.dry_run:
        return 0
    write_seed_rows(alias_path, fields, out)
    print(f"wrote {alias_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

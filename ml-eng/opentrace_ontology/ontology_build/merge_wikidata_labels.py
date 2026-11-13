#!/usr/bin/env python3
"""Merge Wikidata labels into ontology_aliases.csv via Wikidata QID only.

Join path: lab_*.json item QID → ontology_codes.csv (scheme=wikidata) → ontology_id.
No AGROVOC P2004 or name matching.

African langs {hau,yor,ibo,amh,orm,wol,ful} get review_status=unreviewed.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import unicodedata
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, ".."))
ML_ENG = os.path.abspath(os.path.join(PKG, ".."))
REPO = os.path.abspath(os.path.join(ML_ENG, ".."))
SEEDS = os.path.join(PKG, "seeds")
DEFAULT_WD = os.path.join(REPO, "opentrace_ontology", "raw", "wikidata")
DEFAULT_ISO = os.path.join(HERE, "inputs", "iso3166.csv")

if ML_ENG not in sys.path:
    sys.path.insert(0, ML_ENG)
from opentrace_ontology.ontology_build.seed_io import (  # noqa: E402
    open_seed,
    resolve_seed_path,
    write_seed_rows,
)

ISO1_TO3 = {
    "en": "eng",
    "fr": "fra",
    "pt": "por",
    "ar": "arb",
    "sw": "swh",
    "ha": "hau",
    "yo": "yor",
    "ig": "ibo",
    "am": "amh",
    "om": "orm",
    "zu": "zul",
    "xh": "xho",
    "wo": "wol",
    "tw": "twi",
    "ak": "aka",
    "rw": "kin",
    "lg": "lug",
    "so": "som",
    "mg": "mlg",
    "ln": "lin",
    "sn": "sna",
    "ff": "ful",
    "ti": "tir",
}
LAT_RE = re.compile(r"^[A-Z][a-z]+ [a-z]+(?: (?:subsp|var)\. [a-z]+)?$")
UN_LANGS = frozenset({"eng", "fra", "por", "arb", "spa"})
REVIEW_LANGS = frozenset({"hau", "yor", "ibo", "amh", "orm", "wol", "ful"})
ALLOW_TYPES = frozenset(
    {
        "crop",
        "animal",
        "product",
        "by_product",
        "crop_group",
        "animal_group",
        "input",
        "commodity",
        "variety",
        "breed",
        "disease_pest",
    }
)
ALIAS_FIELDS = [
    "ontology_id",
    "alias",
    "language",
    "country_iso3",
    "alias_type",
    "source",
    "review_status",
]


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKC", (s or "").lower().strip())
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return " ".join(s.split())


def qid(uri: str) -> str:
    return uri.rstrip("/").split("/")[-1]


def load_geo_blocklist(iso_path: str) -> set[str]:
    blocked: set[str] = set()
    if not os.path.isfile(iso_path):
        return blocked
    with open(iso_path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            for key in ("iso2", "iso3", "name", "official_name", "common_name"):
                v = (row.get(key) or "").strip()
                if v:
                    blocked.add(fold(v))
    for extra in (
        "ivory coast",
        "cote d'ivoire",
        "côte d'ivoire",
        "cote d ivoire",
        "tanzania",
        "united republic of tanzania",
        "kenya",
        "uganda",
        "senegal",
        "nigeria",
        "ghana",
        "ethiopia",
        "india",
        "canada",
    ):
        blocked.add(fold(extra))
    return blocked


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", default=SEEDS)
    ap.add_argument("--wikidata-dir", default=DEFAULT_WD)
    ap.add_argument("--iso3166", default=DEFAULT_ISO)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max-collisions", type=int, default=3)
    ap.add_argument(
        "--langs",
        default="ha,yo,ig,am,sw,fr,pt,ar",
        help="ISO 639-1 langs to merge (comma-separated)",
    )
    args = ap.parse_args()

    if not os.path.isdir(args.wikidata_dir):
        print(f"SKIP: missing wikidata dir {args.wikidata_dir}", file=sys.stderr)
        return 0

    codes_path = resolve_seed_path(args.seeds, "ontology_codes")
    ent_path = resolve_seed_path(args.seeds, "ontology_entities")
    alias_path = resolve_seed_path(args.seeds, "ontology_aliases")

    # QID → ontology_ids (scheme=wikidata only)
    by_wikidata: dict[str, set[str]] = defaultdict(set)
    with open_seed(codes_path) as f:
        for row in csv.DictReader(f):
            if (row.get("scheme") or "").strip() != "wikidata":
                continue
            code = (row.get("code") or "").strip()
            if code:
                by_wikidata[code].add(row["ontology_id"])

    if not by_wikidata:
        print("SKIP: no wikidata codes in ontology_codes.csv", file=sys.stderr)
        return 0

    sci_by_id: dict[str, str] = {}
    type_by_id: dict[str, str] = {}
    with open_seed(ent_path) as f:
        for row in csv.DictReader(f):
            sci_by_id[row["ontology_id"]] = fold(row.get("scientific_name") or "")
            type_by_id[row["ontology_id"]] = (row.get("entity_type") or "").strip()

    geo_blocked = load_geo_blocklist(args.iso3166)

    with open_seed(alias_path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    # Ensure review_status column on all existing rows
    for r in rows:
        r.setdefault("review_status", "")

    def akey(r: dict) -> tuple:
        return (
            r["ontology_id"],
            fold(r["alias"]),
            r.get("language") or "",
            r.get("country_iso3") or "",
        )

    existing = {akey(r) for r in rows}
    label_hits: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        if r.get("alias_type") != "misspelling":
            label_hits[fold(r["alias"])].add(r["ontology_id"])

    want_langs = {x.strip() for x in args.langs.split(",") if x.strip()}
    added = skipped_latin = skipped_short = skipped_map = skipped_collide = 0
    skipped_type = skipped_geo = 0
    new_rows: list[dict] = []

    for iso1, iso3 in ISO1_TO3.items():
        if iso1 not in want_langs:
            continue
        path = os.path.join(args.wikidata_dir, f"lab_{iso1}.json")
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8") as f:
            bindings = json.load(f)
        for b in bindings:
            item = qid(b["item"]["value"])
            label = (b.get("l") or b.get("label") or {}).get("value") or ""
            label = label.strip()
            if not label:
                continue
            if len(label) <= 2:
                skipped_short += 1
                continue
            if LAT_RE.match(label):
                skipped_latin += 1
                continue
            fl = fold(label)
            if fl in geo_blocked:
                skipped_geo += 1
                continue
            oids = {o for o in by_wikidata.get(item, set()) if type_by_id.get(o) in ALLOW_TYPES}
            if not oids:
                skipped_map += 1
                continue
            if len(oids) > args.max_collisions:
                skipped_collide += 1
                continue
            prior = label_hits.get(fl, set())
            if prior and len(prior | oids) > args.max_collisions:
                skipped_collide += 1
                continue
            typ = "local" if iso3 not in UN_LANGS else "synonym"
            review = "unreviewed" if iso3 in REVIEW_LANGS else ""
            for oid in sorted(oids):
                if type_by_id.get(oid) not in ALLOW_TYPES:
                    skipped_type += 1
                    continue
                if sci_by_id.get(oid) and fl == sci_by_id[oid]:
                    skipped_latin += 1
                    continue
                row = {
                    "ontology_id": oid,
                    "alias": label,
                    "language": iso3,
                    "country_iso3": "",
                    "alias_type": typ,
                    "source": "wikidata",
                    "review_status": review,
                }
                k = akey(row)
                if k in existing:
                    continue
                existing.add(k)
                label_hits[fl].add(oid)
                new_rows.append(row)
                added += 1

    print(
        f"wikidata added={added} skipped_latin={skipped_latin} "
        f"skipped_short={skipped_short} skipped_map={skipped_map} "
        f"skipped_collide={skipped_collide} skipped_geo={skipped_geo} "
        f"skipped_type={skipped_type} qid_codes={len(by_wikidata)}"
    )
    if args.dry_run or not new_rows:
        # Still rewrite header with review_status if missing
        if "review_status" not in (rows[0] if rows else {}):
            pass
        if not new_rows and not args.dry_run:
            write_seed_rows(alias_path, ALIAS_FIELDS, rows)
            print(f"wrote {alias_path} (review_status column ensured, +0)")
        return 0
    rows.extend(new_rows)
    write_seed_rows(alias_path, ALIAS_FIELDS, rows)
    print(f"wrote {alias_path} (+{added})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

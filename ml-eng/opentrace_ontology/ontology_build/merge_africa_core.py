#!/usr/bin/env python3
"""Upsert curated africa_core.txt into package seeds (entities + aliases).

Curated rows win on (ontology_id, alias_norm, language, country_iso3).
Usage:
  python ontology_build/merge_africa_core.py
  python ontology_build/merge_africa_core.py --dry-run
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import unicodedata

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, ".."))
ML_ENG = os.path.abspath(os.path.join(PKG, ".."))
SEEDS = os.path.join(PKG, "seeds")
DEFAULT_CORE = os.path.join(HERE, "curated", "africa_core.txt")

if ML_ENG not in sys.path:
    sys.path.insert(0, ML_ENG)
from opentrace_ontology.ontology_build.seed_io import (  # noqa: E402
    open_seed,
    resolve_seed_path,
    write_seed_rows,
)

UN_LANGS = frozenset({"eng", "fra", "por", "arb", "spa"})


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKC", (s or "").lower().strip())
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return " ".join(s.split())


def parse_core(path: str) -> tuple[list[dict], list[dict]]:
    entities: list[dict] = []
    aliases: list[dict] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.strip() or line.startswith("#"):
                continue
            parts = (line.rstrip("\n").split("|") + [""] * 8)[:8]
            oid, etype, en, sci, parent, _der, at, al = [x.strip() for x in parts]
            if not oid or not en:
                continue
            attrs: dict = {}
            for kv in filter(None, at.split(",")):
                k, _, v = kv.partition("=")
                attrs[k] = v.split(";") if ";" in v else v
            entities.append(
                {
                    "ontology_id": oid,
                    "entity_type": etype,
                    "canonical_name_en": en,
                    "scientific_name": sci.split(";")[0].strip() if sci else "",
                    "parent_id": parent,
                    "attributes": json.dumps(attrs, ensure_ascii=False, sort_keys=True),
                    "primary_source": "opentrace_curated",
                }
            )
            aliases.append(
                {
                    "ontology_id": oid,
                    "alias": en,
                    "language": "eng",
                    "country_iso3": "",
                    "alias_type": "preferred",
                    "source": "opentrace_curated",
                }
            )
            if sci and ";" not in sci:
                aliases.append(
                    {
                        "ontology_id": oid,
                        "alias": sci,
                        "language": "lat",
                        "country_iso3": "",
                        "alias_type": "scientific",
                        "source": "opentrace_curated",
                    }
                )
            for a in filter(None, al.split(";")):
                lc, _, txt = a.partition(":")
                lang, _, ctry = lc.partition("-")
                txt = txt.split("(")[0].strip().strip(";,").strip()
                if not txt or len(txt) > 120:
                    continue
                typ = "local" if lang not in UN_LANGS else "synonym"
                aliases.append(
                    {
                        "ontology_id": oid,
                        "alias": txt,
                        "language": lang,
                        "country_iso3": ctry,
                        "alias_type": typ,
                        "source": "opentrace_curated",
                    }
                )
    return entities, aliases


def _key(row: dict) -> tuple:
    return (
        row["ontology_id"],
        fold(row["alias"]),
        row.get("language") or "",
        row.get("country_iso3") or "",
    )


def load_csv(path: str) -> tuple[list[str], list[dict]]:
    with open_seed(path) as f:
        r = csv.DictReader(f)
        rows = list(r)
        return list(r.fieldnames or []), rows


def write_csv(path: str, fields: list[str], rows: list[dict]) -> None:
    write_seed_rows(path, fields, rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--core", default=DEFAULT_CORE)
    ap.add_argument("--seeds", default=SEEDS)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if not os.path.isfile(args.core):
        print(f"missing {args.core}", file=sys.stderr)
        return 2

    core_ents, core_aliases = parse_core(args.core)
    ent_path = resolve_seed_path(args.seeds, "ontology_entities")
    alias_path = resolve_seed_path(args.seeds, "ontology_aliases")
    ent_fields, ents = load_csv(ent_path)
    alias_fields, aliases = load_csv(alias_path)

    ent_by_id = {e["ontology_id"]: e for e in ents}
    ent_added = ent_updated = 0
    for e in core_ents:
        prev = ent_by_id.get(e["ontology_id"])
        if prev is None:
            ents.append(e)
            ent_by_id[e["ontology_id"]] = e
            ent_added += 1
        else:
            # Keep existing attributes/parent if already present; refresh canonical from curated
            prev["canonical_name_en"] = e["canonical_name_en"] or prev["canonical_name_en"]
            if e["scientific_name"]:
                prev["scientific_name"] = e["scientific_name"]
            if e["parent_id"] and not prev.get("parent_id"):
                prev["parent_id"] = e["parent_id"]
            if prev.get("primary_source") != "opentrace_curated":
                # Do not steal AGROVOC primary_source; only ensure entity exists
                pass
            else:
                ent_updated += 1

    by_key: dict[tuple, dict] = {}
    order: list[tuple] = []
    for row in aliases:
        k = _key(row)
        if k not in by_key:
            order.append(k)
        by_key[k] = row

    alias_upserted = alias_added = 0
    for row in core_aliases:
        k = _key(row)
        if k in by_key:
            prev = by_key[k]
            if prev.get("source") == "opentrace_curated" or row["source"] == "opentrace_curated":
                by_key[k] = row  # curated wins
                alias_upserted += 1
        else:
            by_key[k] = row
            order.append(k)
            alias_added += 1

    out_aliases = [by_key[k] for k in order]
    print(
        f"africa_core entities={len(core_ents)} aliases={len(core_aliases)} | "
        f"ent +{ent_added} ~{ent_updated} | alias +{alias_added} upsert={alias_upserted} | "
        f"total_aliases={len(out_aliases)}"
    )
    if args.dry_run:
        return 0
    write_csv(ent_path, ent_fields, ents)
    write_csv(alias_path, alias_fields, out_aliases)
    print(f"wrote {ent_path}")
    print(f"wrote {alias_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

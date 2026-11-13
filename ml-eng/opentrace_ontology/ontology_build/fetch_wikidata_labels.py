#!/usr/bin/env python3
"""Resumable Wikidata label fetch via wbgetentities (more reliable than bulk SPARQL).

Reads QIDs from bundle core.json (AGROVOC-linked items) or builds a minimal core
via SPARQL if missing. Writes lab_<iso639-1>.json files compatible with
merge_wikidata_labels.py.

Default langs: ha yo ig am sw fr pt ar (African + UN working set for Ask ADZA).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, ".."))
ML_ENG = os.path.abspath(os.path.join(PKG, ".."))
REPO = os.path.abspath(os.path.join(ML_ENG, ".."))
DEFAULT_OUT = os.path.join(REPO, "opentrace_ontology", "raw", "wikidata")
UA = "OpenTraceOntologyBuilder/1.0 (offline enrichment; vernacular labels)"


def http_json(url: str, retries: int = 5) -> dict | list | None:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for i in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.load(resp)
        except Exception as e:
            print(f"retry {e}", file=sys.stderr)
            time.sleep(5 * (i + 1))
    return None


def ensure_core(out_dir: str) -> str:
    path = os.path.join(out_dir, "core.json")
    # Prefer already-fetched core from bundle build/raw
    alt = os.path.join(REPO, "opentrace_ontology", "build", "raw", "wikidata", "core.json")
    if not os.path.isfile(path) and os.path.isfile(alt):
        os.makedirs(out_dir, exist_ok=True)
        with open(alt, encoding="utf-8") as f:
            data = json.load(f)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        print(f"copied core from {alt} ({len(data)} rows)")
        return path
    if os.path.isfile(path):
        return path
    # Minimal SPARQL core
    os.makedirs(out_dir, exist_ok=True)
    q = (
        "SELECT ?item ?agrovoc WHERE { ?item wdt:P2004 ?agrovoc. } "
        "LIMIT 20000"
    )
    url = "https://query.wikidata.org/sparql?" + urllib.parse.urlencode(
        {"query": q, "format": "json"}
    )
    data = http_json(url)
    if not data:
        raise SystemExit("failed to fetch Wikidata core")
    bindings = data.get("results", {}).get("bindings", [])
    with open(path, "w", encoding="utf-8") as f:
        json.dump(bindings, f)
    print(f"wrote {path} ({len(bindings)})")
    return path


def qids_from_core(core_path: str) -> list[str]:
    with open(core_path, encoding="utf-8") as f:
        bindings = json.load(f)
    ids = []
    for b in bindings:
        uri = b.get("item", {}).get("value") or ""
        if uri:
            ids.append(uri.rstrip("/").split("/")[-1])
    return sorted(set(ids))


def fetch_labs(qids: list[str], langs: list[str], out_dir: str, batch: int = 40) -> None:
    # Accumulate per-lang bindings in merge-compatible shape:
    # [{"item":{"value":"http://www.wikidata.org/entity/Q…"}, "l":{"value":"…"}}]
    buckets: dict[str, list] = {l: [] for l in langs}
    for i in range(0, len(qids), batch):
        chunk = qids[i : i + batch]
        params = {
            "action": "wbgetentities",
            "ids": "|".join(chunk),
            "props": "labels|aliases",
            "languages": "|".join(langs),
            "format": "json",
        }
        url = "https://www.wikidata.org/w/api.php?" + urllib.parse.urlencode(params)
        data = http_json(url)
        if not data or "entities" not in data:
            print(f"batch {i} failed", file=sys.stderr)
            continue
        for qid, ent in data["entities"].items():
            if "missing" in ent:
                continue
            uri = f"http://www.wikidata.org/entity/{qid}"
            labels = ent.get("labels") or {}
            aliases = ent.get("aliases") or {}
            for lang in langs:
                seen: set[str] = set()
                if lang in labels:
                    v = labels[lang].get("value") or ""
                    if v and v not in seen:
                        buckets[lang].append({"item": {"value": uri}, "l": {"value": v}})
                        seen.add(v)
                for a in aliases.get(lang) or []:
                    v = a.get("value") or ""
                    if v and v not in seen:
                        buckets[lang].append({"item": {"value": uri}, "l": {"value": v}})
                        seen.add(v)
        print(f"batch {i // batch + 1}/{(len(qids) + batch - 1) // batch}", flush=True)
        time.sleep(0.4)

    for lang, rows in buckets.items():
        path = os.path.join(out_dir, f"lab_{lang}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f)
        print(f"wrote {path} ({len(rows)})")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument(
        "--langs",
        default="ha,yo,ig,am,sw,fr,pt,ar",
        help="comma-separated ISO 639-1 codes",
    )
    ap.add_argument("--limit", type=int, default=0, help="0 = all QIDs from core")
    ap.add_argument("--batch", type=int, default=40)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    core = ensure_core(args.out)
    qids = qids_from_core(core)
    if args.limit:
        qids = qids[: args.limit]
    langs = [x.strip() for x in args.langs.split(",") if x.strip()]
    print(f"fetching labels for {len(qids)} items × {langs}")
    fetch_labs(qids, langs, args.out, batch=args.batch)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

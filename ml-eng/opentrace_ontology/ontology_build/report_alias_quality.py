#!/usr/bin/env python3
"""Alias quality report + size/latency gates.

Exit 1 if --fail and any of:
- non-eng preferred Latin rate >= --max-latin-preferred (default 0.10)
- priority vernacular gaps remain (not allowlisted)
- sqlite size grows >25% vs baseline JSON
- matcher cold-start > 1.0 s
Allowlist rows must include reviewer, reviewed_on, reason.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import statistics
import sys
import time
from collections import Counter, defaultdict

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
)
DEFAULT_ALLOW = os.path.join(HERE, "curated", "vernacular_gaps_allowlist.csv")
DEFAULT_OUT = os.path.join(PKG, "data", "reports", "alias_quality.md")
DEFAULT_BASELINE = os.path.join(PKG, "data", "reports", "alias_quality_baseline.json")
DEFAULT_SQLITE = os.path.join(PKG, "data", "ontology.sqlite")

LAT_RE = re.compile(r"^[A-Z][a-z]+ [a-z]+(?: (?:subsp|var)\. [a-z]+)?$")
PRIORITY = [
    "OT:CROP:cassava",
    "OT:CROP:maize",
    "OT:CROP:rice",
    "OT:CROP:yam",
    "OT:CROP:sorghum",
    "OT:CROP:pearl_millet",
    "OT:CROP:finger_millet",
    "OT:CROP:cowpea",
    "OT:CROP:groundnut",
    "OT:CROP:cocoa",
    "OT:CROP:coffee",
    "OT:CROP:oil_palm",
    "OT:CROP:plantain",
    "OT:ANIMAL:cattle",
    "OT:ANIMAL:goat",
    "OT:ANIMAL:chicken",
    "OT:PRODUCT:garri",
    "OT:PRODUCT:palm_oil",
]
REQ_LANGS = ["hau", "yor", "ibo", "swh", "fra"]


def core_ids(path: str) -> set[str]:
    ids: set[str] = set()
    if not os.path.isfile(path):
        return ids
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.strip() or line.startswith("#"):
                continue
            ids.add(line.split("|", 1)[0].strip())
    return ids


def load_allowlist(path: str) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    if not os.path.isfile(path):
        return out
    with open(path, encoding="utf-8", newline="") as f:
        for i, row in enumerate(csv.DictReader(f), start=2):
            oid = (row.get("ontology_id") or "").strip()
            lang = (row.get("language") or "").strip()
            if not oid and not lang:
                continue
            for col in ("reviewer", "reviewed_on", "reason"):
                if not (row.get(col) or "").strip():
                    raise SystemExit(
                        f"allowlist {path}:{i} missing required column {col}"
                    )
            out.add((oid, lang))
    return out


def measure_perf(sqlite_path: str) -> dict:
    size_mb = (
        round(os.path.getsize(sqlite_path) / (1024 * 1024), 3)
        if os.path.isfile(sqlite_path)
        else 0.0
    )
    # Cold matcher: clear caches by fresh import in subprocess-like reset
    import importlib

    # Ensure package on path
    ml_eng = os.path.abspath(os.path.join(PKG, ".."))
    if ml_eng not in sys.path:
        sys.path.insert(0, ml_eng)

    import opentrace_ontology as oo
    from opentrace_ontology import _matcher, _data

    _matcher.automaton.cache_clear()
    _data.light_entities.cache_clear()
    _data.candidates.cache_clear()
    try:
        _data.matcher_keys.cache_clear()
    except Exception:
        pass

    t0 = time.perf_counter()
    _matcher.automaton()
    cold_s = time.perf_counter() - t0

    q = "What is the current state of casssava in Nigeria"
    oo.plan_query(q)  # warm
    samples: list[float] = []
    for _ in range(200):
        t1 = time.perf_counter()
        oo.plan_query(q)
        samples.append(time.perf_counter() - t1)
    samples.sort()
    p95 = samples[int(0.95 * (len(samples) - 1))]
    return {
        "sqlite_mb": size_mb,
        "matcher_cold_s": round(cold_s, 4),
        "plan_query_warm_p95_ms": round(p95 * 1000, 4),
        "plan_query_warm_mean_ms": round(statistics.mean(samples) * 1000, 4),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", default=SEEDS)
    ap.add_argument("--core", default=DEFAULT_CORE)
    ap.add_argument("--allowlist", default=DEFAULT_ALLOW)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--baseline", default=DEFAULT_BASELINE)
    ap.add_argument("--sqlite", default=DEFAULT_SQLITE)
    ap.add_argument("--write-baseline", action="store_true")
    ap.add_argument("--fail", action="store_true")
    ap.add_argument("--max-latin-preferred", type=float, default=0.10)
    ap.add_argument("--max-size-growth", type=float, default=0.25)
    ap.add_argument("--max-cold-s", type=float, default=1.0)
    ap.add_argument("--skip-perf", action="store_true")
    args = ap.parse_args()

    alias_path = resolve_seed_path(args.seeds, "ontology_aliases")
    lang_c: Counter[str] = Counter()
    type_c: Counter[str] = Counter()
    src_c: Counter[str] = Counter()
    pref_non_eng = 0
    pref_latin = 0
    by_oid_lang: dict[str, set[str]] = defaultdict(set)

    with open_seed(alias_path) as f:
        for row in csv.DictReader(f):
            lang = row.get("language") or ""
            lang_c[lang] += 1
            type_c[row.get("alias_type") or ""] += 1
            src_c[row.get("source") or ""] += 1
            by_oid_lang[row["ontology_id"]].add(lang)
            if row.get("alias_type") == "preferred" and lang not in ("eng", "lat", ""):
                pref_non_eng += 1
                if LAT_RE.match((row.get("alias") or "").strip()):
                    pref_latin += 1

    rate = (pref_latin / pref_non_eng) if pref_non_eng else 0.0
    core = core_ids(args.core)
    allow = load_allowlist(args.allowlist)

    gaps: list[str] = []
    for oid in PRIORITY:
        for lang in REQ_LANGS:
            if lang in by_oid_lang.get(oid, set()):
                continue
            if (oid, lang) in allow:
                continue
            gaps.append(f"{oid}|{lang}")

    perf = {
        "sqlite_mb": 0.0,
        "matcher_cold_s": 0.0,
        "plan_query_warm_p95_ms": 0.0,
        "plan_query_warm_mean_ms": 0.0,
    }
    if not args.skip_perf:
        perf = measure_perf(args.sqlite)

    if args.write_baseline:
        os.makedirs(os.path.dirname(args.baseline), exist_ok=True)
        with open(args.baseline, "w", encoding="utf-8") as f:
            json.dump(perf, f, indent=2)
        print(f"wrote baseline {args.baseline}")

    baseline = None
    if os.path.isfile(args.baseline) and not args.write_baseline:
        with open(args.baseline, encoding="utf-8") as f:
            baseline = json.load(f)

    size_growth = None
    if baseline and baseline.get("sqlite_mb"):
        size_growth = (perf["sqlite_mb"] - baseline["sqlite_mb"]) / baseline["sqlite_mb"]

    lines = [
        "# Alias quality report",
        "",
        f"- total_aliases: **{sum(lang_c.values())}**",
        f"- non_eng_preferred: **{pref_non_eng}**",
        f"- non_eng_preferred_latin_looking: **{pref_latin}** ({rate:.1%})",
        f"- africa_core_entities: **{len(core)}**",
        f"- priority_vernacular_gaps: **{len(gaps)}**",
        f"- sqlite_mb: **{perf['sqlite_mb']}**",
        f"- matcher_cold_s: **{perf['matcher_cold_s']}**",
        f"- plan_query_warm_p95_ms: **{perf['plan_query_warm_p95_ms']}**",
        f"- plan_query_warm_mean_ms: **{perf['plan_query_warm_mean_ms']}**",
    ]
    if baseline:
        lines.append(f"- baseline_sqlite_mb: **{baseline.get('sqlite_mb')}**")
        lines.append(f"- baseline_matcher_cold_s: **{baseline.get('matcher_cold_s')}**")
        lines.append(
            f"- baseline_plan_query_warm_p95_ms: **{baseline.get('plan_query_warm_p95_ms')}**"
        )
        if size_growth is not None:
            lines.append(f"- sqlite_size_growth: **{size_growth:.1%}**")
    lines += [
        "",
        "## By language (top 25)",
        "",
        "| language | count |",
        "|---|---:|",
    ]
    for lang, n in lang_c.most_common(25):
        lines.append(f"| {lang or '(empty)'} | {n} |")
    lines += ["", "## By alias_type", "", "| alias_type | count |", "|---|---:|"]
    for t, n in type_c.most_common():
        lines.append(f"| {t or '(empty)'} | {n} |")
    lines += ["", "## By source", "", "| source | count |", "|---|---:|"]
    for s, n in src_c.most_common():
        lines.append(f"| {s or '(empty)'} | {n} |")
    lines += ["", "## Priority vernacular gaps", ""]
    if gaps:
        lines.append("| ontology_id | missing_lang |")
        lines.append("|---|---|")
        for g in gaps:
            oid, lang = g.split("|", 1)
            lines.append(f"| {oid} | {lang} |")
    else:
        lines.append("_none_")
    lines.append("")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"wrote {args.out}")
    print(
        f"latin_preferred_rate={rate:.4f} gaps={len(gaps)} "
        f"local={type_c.get('local', 0)} hau={lang_c.get('hau', 0)} yor={lang_c.get('yor', 0)} "
        f"sqlite_mb={perf['sqlite_mb']} cold_s={perf['matcher_cold_s']} "
        f"p95_ms={perf['plan_query_warm_p95_ms']}"
    )

    if not args.fail:
        return 0
    if rate >= args.max_latin_preferred:
        print(
            f"FAIL latin preferred rate {rate:.1%} >= {args.max_latin_preferred:.0%}",
            file=sys.stderr,
        )
        return 1
    if gaps:
        print(f"FAIL {len(gaps)} priority vernacular gaps", file=sys.stderr)
        return 1
    if perf["matcher_cold_s"] > args.max_cold_s:
        print(
            f"FAIL matcher cold-start {perf['matcher_cold_s']:.3f}s > {args.max_cold_s}s",
            file=sys.stderr,
        )
        return 1
    if size_growth is not None and size_growth > args.max_size_growth:
        print(
            f"FAIL sqlite size growth {size_growth:.1%} > {args.max_size_growth:.0%}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

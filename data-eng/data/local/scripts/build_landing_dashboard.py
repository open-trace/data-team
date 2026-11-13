#!/usr/bin/env python3
"""
Build dashboard CSVs from the landing datapoint counts.

Labels each table with one indicator class and family, fills years that the
first count left blank (extra observation-date columns, then the table name),
and writes inventories that each sum to the same grand total.

Usage (from data-eng/):
  python data/local/scripts/build_landing_dashboard.py
"""
from __future__ import annotations

import csv
import os
import re
import sys
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from count_landing_datapoints import (  # noqa: E402
    PERIOD_YEAR,
    WIDE_YEAR,
    _load_dotenv,
    count_table,
    load_map,
    table_columns,
)

CATALOG_PATH = SCRIPTS_DIR / "indicator_class_catalog.yaml"
COUNTS_PATH = REPO_ROOT / "data" / "local" / "out" / "landing_datapoint_counts.csv"
SUMMARY_PATH = REPO_ROOT / "data" / "local" / "out" / "landing_datapoint_count_summary.csv"
OUT_DIR = REPO_ROOT / "data" / "local" / "out" / "dashboard"

# Date columns the first count did not use. Tables with no year and one of
# these columns are recounted so the year can vary by row.
EXTRA_DATE_COLUMNS = (
    "plantingdate",
    "harvestdate",
    "eventdate",
    "projection_start",
    "start_date",
    "first_period_date",
    "last_period_date",
)
VALUE_COLUMNS = {
    "value",
    "obs_value",
    "price",
    "usdprice",
    "mp_price",
    "indicator_value",
    "common_unit_price",
    "common_currency_price",
}
UNKNOWN = "unknown"
UNCLASSIFIED = "unclassified"
_EMPTY = {"", "none", "null", "nan"}
_STAMP = re.compile(r"(?:19|20)\d{2}_\d{2}_\d{2}T\d+", re.IGNORECASE)


def _key_file() -> None:
    if (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip():
        return
    key = REPO_ROOT / "data" / "local" / "keys" / "opentrace-bq-key.json"
    if key.exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(key)


def _blank(value: object) -> bool:
    return str(value or "").strip().lower() in _EMPTY


def year_from_name(table: str) -> str:
    """First observation year written in a table id. Download stamps are ignored."""
    cleaned = _STAMP.sub(" ", table)
    if re.search(r"breeding", cleaned, re.IGNORECASE):
        match = re.search(r"((?:19|20)\d{2})[_-](\d{2,4})", cleaned)
        if match:
            return match.group(1)
    match = re.search(r"((?:19|20)\d{2})[_-]((?:19|20)\d{2})", cleaned)
    if match:
        return match.group(1)
    match = re.search(r"((?:19|20)\d{2})-(\d{2})\b", cleaned)
    if match:
        return match.group(1)
    match = re.search(r"(?:19|20)\d{2}", cleaned)
    return match.group(0) if match else ""


def load_catalog() -> tuple[dict, dict, list[tuple[str, re.Pattern[str]]]]:
    data = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    classes = data.get("classes") or {}
    families = data.get("families") or {}
    patterns: list[tuple[str, re.Pattern[str]]] = []
    for row in data.get("patterns") or []:
        family = str(row.get("family") or "").strip()
        pattern = str(row.get("pattern") or "").strip()
        if family and pattern:
            patterns.append((family, re.compile(pattern, re.IGNORECASE)))
    return classes, families, patterns


def match_family(text: str, patterns: list[tuple[str, re.Pattern[str]]]) -> str:
    folded = text.replace("_", " ")
    for family, pattern in patterns:
        if pattern.search(text) or pattern.search(folded):
            return family
    return ""


def classify_table(
    dataset: str,
    table: str,
    indicators: set[str],
    families: dict,
    patterns: list[tuple[str, re.Pattern[str]]],
) -> tuple[str, str]:
    family = match_family(f"{dataset} {table}", patterns)
    if not family and indicators:
        family = match_family(" ".join(sorted(indicators)), patterns)
    if not family or family not in families:
        return UNCLASSIFIED, UNCLASSIFIED
    return str(families[family].get("class_id") or UNCLASSIFIED), family


def column_role(name: str, role_sets: dict[str, set[str]]) -> str:
    low = name.lower()
    if low in role_sets["year"] or WIDE_YEAR.match(name) or PERIOD_YEAR.match(name):
        return "year"
    if low in role_sets["country"]:
        return "country"
    if low in role_sets["indicator"]:
        return "indicator"
    if low in role_sets["source"]:
        return "source"
    if low in VALUE_COLUMNS:
        return "value"
    return "other"


def has_extra_date(columns: list[tuple[str, str]]) -> bool:
    names = {name.lower() for name, _typ in columns}
    return any(col in names for col in EXTRA_DATE_COLUMNS)


def normalize_country(value: object) -> str:
    text = "" if _blank(value) else str(value).strip()
    return text or UNKNOWN


def normalize_year(value: object) -> str:
    text = "" if _blank(value) else str(value).strip()
    return text or UNKNOWN


def write_catalog(path: Path, classes: dict, families: dict) -> None:
    fields = [
        "class_id",
        "class_name",
        "opentrace_definition",
        "operational_definition",
        "family_id",
        "concept_family",
        "measurement_forms",
    ]
    rows = []
    for family_id, spec in families.items():
        class_id = str(spec.get("class_id") or "")
        cls = classes.get(class_id) or {}
        rows.append({
            "class_id": class_id,
            "class_name": cls.get("name") or "",
            "opentrace_definition": cls.get("opentrace_definition") or "",
            "operational_definition": cls.get("operational_definition") or "",
            "family_id": family_id,
            "concept_family": spec.get("concept_family") or "",
            "measurement_forms": spec.get("measurement_forms") or "",
        })
    rows.sort(key=lambda row: (row["class_id"], row["family_id"]))
    _write_csv(path, fields, rows)


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    import argparse

    from google.cloud import bigquery

    _load_dotenv()
    _key_file()
    parser = argparse.ArgumentParser(description="Build landing class/country/year dashboard CSVs")
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()

    if not COUNTS_PATH.exists() or not SUMMARY_PATH.exists():
        print("Missing landing count CSVs in data/local/out/", file=sys.stderr)
        return 1
    project = (os.environ.get("BQ_PROJECT") or "").strip()
    if not project:
        print("Set BQ_PROJECT (e.g. in data/local/.env)", file=sys.stderr)
        return 1

    classes, families, patterns = load_catalog()
    rules = load_map()
    role_sets = {
        "year": {str(name).lower() for name in rules.get("year_columns") or []},
        "country": {str(name).lower() for name in rules.get("country_columns") or []},
        "indicator": {str(name).lower() for name in rules.get("indicator_columns") or []},
        "source": {str(name).lower() for name in rules.get("source_columns") or []},
    }
    summary: dict[tuple[str, str], dict] = {}
    with SUMMARY_PATH.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            summary[(row["dataset"], row["table"])] = row
    print(f"Summary tables: {len(summary)}")

    client = bigquery.Client(project=project)
    columns_by_table: dict[tuple[str, str], list[tuple[str, str]]] = {}
    datasets = sorted({dataset for dataset, _table in summary})
    for dataset in datasets:
        print(f"Reading columns for {dataset}")
        for table, cols in table_columns(client, project, dataset).items():
            columns_by_table[(dataset, table)] = cols

    indicators: dict[tuple[str, str], set[str]] = defaultdict(set)
    with COUNTS_PATH.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            key = (row["dataset"], row["table"])
            if not _blank(row.get("indicator")) and len(indicators[key]) < 30:
                indicators[key].add(str(row["indicator"]))

    labels: dict[tuple[str, str], tuple[str, str]] = {}
    for key, row in summary.items():
        labels[key] = classify_table(key[0], key[1], indicators.get(key, set()), families, patterns)

    recount: list[tuple[str, str]] = []
    for key, row in summary.items():
        if (row.get("year_mode") or "") != "none":
            continue
        if has_extra_date(columns_by_table.get(key, [])):
            recount.append(key)
    print(f"Recounting {len(recount)} tables that have an unused observation date")

    recounted: dict[tuple[str, str], list[dict]] = {}
    recount_errors: dict[tuple[str, str], str] = {}
    lock = threading.Lock()
    done = 0

    def _recount(key: tuple[str, str]) -> None:
        nonlocal done
        dataset, table = key
        rows, info = count_table(
            client,
            project,
            dataset,
            table,
            columns_by_table.get(key, []),
            rules,
        )
        with lock:
            done += 1
            if info.get("error"):
                recount_errors[key] = info["error"]
            else:
                recounted[key] = rows
            if done % 25 == 0 or done == len(recount):
                print(f"  recounted {done}/{len(recount)}")

    if recount:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = [pool.submit(_recount, key) for key in recount]
            for future in as_completed(futures):
                future.result()

    agg: Counter[tuple[str, str, str, str, str]] = Counter()
    table_n: Counter[tuple[str, str]] = Counter()
    table_basis: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    table_unknown_country: Counter[tuple[str, str]] = Counter()

    def add(key: tuple[str, str], country: object, year: object, basis: str, n: int) -> None:
        class_id, family_id = labels[key]
        country_text = normalize_country(country)
        year_text = normalize_year(year)
        agg[(class_id, family_id, country_text, year_text, basis)] += n
        table_n[key] += n
        table_basis[key][basis] += n
        if country_text == UNKNOWN:
            table_unknown_country[key] += n

    recount_keys = set(recount)
    with COUNTS_PATH.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            key = (row["dataset"], row["table"])
            if key in recounted:
                continue
            info = summary.get(key) or {}
            mode = info.get("year_mode") or "none"
            n = int(row["n"])
            named = year_from_name(key[1])
            if mode in {"column", "wide"} and key not in recount_errors:
                basis = mode if not _blank(row.get("year")) else "unknown"
                add(key, row.get("country"), row.get("year"), basis, n)
            elif not _blank(row.get("year")):
                add(key, row.get("country"), row.get("year"), "column", n)
            elif named:
                add(key, row.get("country"), named, "table_name", n)
            else:
                add(key, row.get("country"), "", "unknown", n)

    for key, rows in recounted.items():
        named = year_from_name(key[1])
        for row in rows:
            n = int(row["n"])
            if _blank(row.get("year")) and named:
                add(key, row.get("country"), named, "table_name", n)
            elif _blank(row.get("year")):
                add(key, row.get("country"), "", "unknown", n)
            else:
                add(key, row.get("country"), row.get("year"), "date_column", n)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_catalog(OUT_DIR / "indicator_class_catalog.csv", classes, families)

    map_rows = []
    for (dataset, table), cols in sorted(columns_by_table.items()):
        class_id, family_id = labels.get((dataset, table), (UNCLASSIFIED, UNCLASSIFIED))
        for name, data_type in cols:
            map_rows.append({
                "dataset": dataset,
                "table": table,
                "column": name,
                "type": data_type,
                "role": column_role(name, role_sets),
                "class_id": class_id,
                "family_id": family_id,
            })
    _write_csv(
        OUT_DIR / "landing_column_map.csv",
        ["dataset", "table", "column", "type", "role", "class_id", "family_id"],
        map_rows,
    )

    detail_rows = [
        {
            "class_id": class_id,
            "family_id": family_id,
            "country": country,
            "year": year,
            "year_basis": basis,
            "n": n,
        }
        for (class_id, family_id, country, year, basis), n in agg.items()
    ]
    detail_rows.sort(key=lambda row: (
        row["class_id"],
        row["family_id"],
        row["country"] == UNKNOWN,
        row["country"],
        row["year"] == UNKNOWN,
        row["year"],
        row["year_basis"],
    ))
    _write_csv(
        OUT_DIR / "datapoints_by_class_country_year.csv",
        ["class_id", "family_id", "country", "year", "year_basis", "n"],
        detail_rows,
    )

    country_n: Counter[str] = Counter()
    year_n: Counter[str] = Counter()
    family_n: Counter[tuple[str, str]] = Counter()
    class_n: Counter[str] = Counter()
    for (class_id, family_id, country, year, _basis), n in agg.items():
        country_n[country] += n
        year_n[year] += n
        family_n[(class_id, family_id)] += n
        class_n[class_id] += n

    def _label_sort(value: str) -> tuple:
        return (value == UNKNOWN, value)

    _write_csv(
        OUT_DIR / "datapoints_by_country.csv",
        ["country", "n"],
        [{"country": country, "n": n} for country, n in sorted(country_n.items(), key=lambda item: _label_sort(item[0]))],
    )
    _write_csv(
        OUT_DIR / "datapoints_by_year.csv",
        ["year", "n"],
        [{"year": year, "n": n} for year, n in sorted(year_n.items(), key=lambda item: _label_sort(item[0]))],
    )

    indicator_rows = []
    for (class_id, family_id), n in sorted(family_n.items()):
        cls = classes.get(class_id) or {}
        fam = families.get(family_id) or {}
        indicator_rows.append({
            "level": "family",
            "class_id": class_id,
            "class_name": cls.get("name") or ("Unclassified" if class_id == UNCLASSIFIED else ""),
            "family_id": family_id,
            "concept_family": fam.get("concept_family") or ("Unclassified" if family_id == UNCLASSIFIED else ""),
            "n": n,
        })
    for class_id, n in sorted(class_n.items()):
        cls = classes.get(class_id) or {}
        indicator_rows.append({
            "level": "class",
            "class_id": class_id,
            "class_name": cls.get("name") or ("Unclassified" if class_id == UNCLASSIFIED else ""),
            "family_id": "",
            "concept_family": "",
            "n": n,
        })
    _write_csv(
        OUT_DIR / "datapoints_by_indicator.csv",
        ["level", "class_id", "class_name", "family_id", "concept_family", "n"],
        indicator_rows,
    )

    coverage = []
    for key, row in sorted(summary.items()):
        class_id, family_id = labels.get(key, (UNCLASSIFIED, UNCLASSIFIED))
        bases = table_basis.get(key) or Counter()
        basis = bases.most_common(1)[0][0] if bases else "unknown"
        total = table_n.get(key, 0)
        country_missing = str(row.get("country_unmapped")) == "True" or (
            total > 0 and table_unknown_country.get(key, 0) == total
        )
        coverage.append({
            "dataset": key[0],
            "table": key[1],
            "datapoints": total,
            "class_id": class_id,
            "family_id": family_id,
            "year_basis": basis,
            "country_missing": country_missing,
            "indicator_missing": str(row.get("indicator_unmapped")) == "True",
        })
    _write_csv(
        OUT_DIR / "coverage_report.csv",
        [
            "dataset",
            "table",
            "datapoints",
            "class_id",
            "family_id",
            "year_basis",
            "country_missing",
            "indicator_missing",
        ],
        coverage,
    )

    grand = sum(agg.values())
    country_sum = sum(country_n.values())
    year_sum = sum(year_n.values())
    family_sum = sum(family_n.values())
    class_sum = sum(class_n.values())
    print(f"Grand total: {grand}")
    print(f"Countries {country_sum}  years {year_sum}  families {family_sum}  classes {class_sum}")
    print(f"Recount errors: {len(recount_errors)}")
    print(f"Wrote {OUT_DIR}")
    if not (grand == country_sum == year_sum == family_sum == class_sum):
        print("Inventory totals do not match", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

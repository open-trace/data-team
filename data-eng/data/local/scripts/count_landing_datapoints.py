#!/usr/bin/env python3
"""
Count datapoints in every base table of BigQuery datasets whose id contains
"landing".

Dimensions come from data/local/scripts/landing_count_map.yaml, which was
locked from the local partition. Wide FAOSTAT year columns (Y1991, ...) are
unpivoted so each non-null cell is one datapoint. Tables with no matching
column still appear, with nulls on the missing dimensions. If there is no
source column, source is `dataset.table` and source_is_fallback is true.

Usage (from data-eng/):
  python data/local/scripts/count_landing_datapoints.py
  python data/local/scripts/count_landing_datapoints.py --datasets landing_dev
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml
from google.cloud import bigquery

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from sync_landing_partitions import base_tables, landing_datasets  # noqa: E402

MAP_PATH = SCRIPTS_DIR / "landing_count_map.yaml"
OUT_DIR = REPO_ROOT / "data" / "local" / "out"
COUNTS_PATH = OUT_DIR / "landing_datapoint_counts.csv"
SUMMARY_PATH = OUT_DIR / "landing_datapoint_count_summary.csv"

WIDE_YEAR = re.compile(r"^Y((?:19|20)\d{2})$", re.IGNORECASE)
PERIOD_YEAR = re.compile(r"^Y(\d{4})(\d{4})$", re.IGNORECASE)

COUNT_FIELDS = [
    "dataset",
    "table",
    "indicator",
    "country",
    "year",
    "source",
    "source_is_fallback",
    "n",
]
SUMMARY_FIELDS = [
    "dataset",
    "table",
    "datapoints",
    "year_mode",
    "indicator_column",
    "country_column",
    "year_column",
    "source_column",
    "source_is_fallback",
    "indicator_unmapped",
    "country_unmapped",
    "year_unmapped",
    "source_unmapped",
    "indicator_null",
    "country_null",
    "year_null",
    "source_null",
    "error",
]


def _load_dotenv() -> None:
    env_file = REPO_ROOT / "data" / "local" / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().replace("export ", "", 1).strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


def quote_bq(name: str) -> str:
    return "`" + str(name).replace("`", "") + "`"


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def load_map() -> dict:
    data = yaml.safe_load(MAP_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit(f"Expected a mapping in {MAP_PATH}")
    return data


def table_columns(client: bigquery.Client, project: str, dataset: str) -> dict[str, list[tuple[str, str]]]:
    sql = f"""
    SELECT c.table_name, c.column_name, c.data_type
    FROM {quote_bq(project)}.{quote_bq(dataset)}.INFORMATION_SCHEMA.COLUMNS c
    JOIN {quote_bq(project)}.{quote_bq(dataset)}.INFORMATION_SCHEMA.TABLES t
      USING (table_name)
    WHERE t.table_type = 'BASE TABLE'
    ORDER BY c.table_name, c.ordinal_position
    """
    out: dict[str, list[tuple[str, str]]] = {}
    for row in client.query(sql).result():
        out.setdefault(row.table_name, []).append((row.column_name, row.data_type))
    return out


def pick_columns(
    columns: list[tuple[str, str]],
    candidates: list[str],
    *,
    string_only: set[str],
) -> list[tuple[str, str]]:
    by_lower: dict[str, tuple[str, str]] = {}
    for name, data_type in columns:
        by_lower.setdefault(name.lower(), (name, data_type))
    found: list[tuple[str, str]] = []
    seen: set[str] = set()
    for candidate in candidates:
        hit = by_lower.get(candidate.lower())
        if hit is None:
            continue
        name, data_type = hit
        if name in seen:
            continue
        if candidate.lower() in string_only and data_type != "STRING":
            continue
        seen.add(name)
        found.append(hit)
    return found


def pick_column(
    columns: list[tuple[str, str]],
    candidates: list[str],
    *,
    string_only: set[str],
) -> tuple[str, str] | None:
    found = pick_columns(columns, candidates, string_only=string_only)
    return found[0] if found else None


def wide_year_columns(columns: list[tuple[str, str]]) -> list[str]:
    names: list[str] = []
    for name, _data_type in columns:
        if WIDE_YEAR.match(name) or PERIOD_YEAR.match(name):
            names.append(name)
    return names


def year_expr(name: str, data_type: str) -> str:
    ident = quote_bq(name)
    if data_type in {"DATE", "DATETIME", "TIMESTAMP"}:
        return f"CAST(EXTRACT(YEAR FROM {ident}) AS STRING)"
    return (
        f"REGEXP_EXTRACT({as_string(name, data_type)}, r'((?:19|20)\\d{{2}})')"
    )


def as_string(name: str, data_type: str) -> str:
    ident = quote_bq(name)
    upper = (data_type or "").upper()
    if upper.startswith("ARRAY") or upper.startswith("STRUCT") or upper in {"JSON", "GEOGRAPHY", "RECORD"}:
        return f"TO_JSON_STRING({ident})"
    return f"CAST({ident} AS STRING)"


def dim_expr(picked: tuple[str, str] | None) -> str:
    if picked is None:
        return "CAST(NULL AS STRING)"
    return as_string(picked[0], picked[1])


def build_sql(
    project: str,
    dataset: str,
    table: str,
    columns: list[tuple[str, str]],
    rules: dict,
) -> tuple[str, dict]:
    string_only = {str(name).lower() for name in rules.get("source_string_only") or []}
    skip_year = {str(name).lower() for name in rules.get("year_skip") or []}
    year_candidates = [
        name for name in (rules.get("year_columns") or []) if str(name).lower() not in skip_year
    ]
    indicator = pick_column(columns, list(rules.get("indicator_columns") or []), string_only=set())
    country = pick_column(columns, list(rules.get("country_columns") or []), string_only=set())
    year = pick_column(columns, year_candidates, string_only=set())
    sources = pick_columns(columns, list(rules.get("source_columns") or []), string_only=string_only)
    wide = [] if year else wide_year_columns(columns)

    indicator_sql = dim_expr(indicator)
    country_sql = dim_expr(country)
    if sources:
        parts = [
            f"NULLIF({as_string(name, data_type)}, '')" for name, data_type in sources
        ]
        source_sql = f"COALESCE({', '.join(parts)})" if len(parts) > 1 else parts[0]
        source_fallback = False
    else:
        source_sql = sql_string(f"{dataset}.{table}")
        source_fallback = True

    full = f"{quote_bq(project)}.{quote_bq(dataset)}.{quote_bq(table)}"
    if wide:
        in_list = ", ".join(quote_bq(name) for name in wide)
        passthrough: list[str] = []
        seen_pass: set[str] = set(wide)
        for picked in (indicator, country, *sources):
            if picked and picked[0] not in seen_pass:
                seen_pass.add(picked[0])
                passthrough.append(quote_bq(picked[0]))
        cast_years = [
            f"CAST({quote_bq(name)} AS STRING) AS {quote_bq(name)}" for name in wide
        ]
        inner = ", ".join(passthrough + cast_years)
        year_sql = f"""
        CASE
          WHEN REGEXP_CONTAINS(year_name, r'(?i)^Y(19|20)\\d{{2}}$')
            THEN REGEXP_EXTRACT(year_name, r'(?i)^Y((?:19|20)\\d{{2}})$')
          WHEN REGEXP_CONTAINS(year_name, r'(?i)^Y\\d{{8}}$')
            THEN CONCAT(
              REGEXP_EXTRACT(year_name, r'(?i)^Y(\\d{{4}})\\d{{4}}$'),
              '-',
              REGEXP_EXTRACT(year_name, r'(?i)^Y\\d{{4}}(\\d{{4}})$')
            )
        END
        """.strip()
        sql = f"""
        SELECT
          {indicator_sql} AS indicator,
          {country_sql} AS country,
          {year_sql} AS year,
          {source_sql} AS source,
          COUNT(*) AS n
        FROM (
          SELECT {inner} FROM {full}
        )
        UNPIVOT (year_value FOR year_name IN ({in_list}))
        GROUP BY 1, 2, 3, 4
        """
        year_mode = "wide"
        year_column = ",".join(wide[:8]) + ("..." if len(wide) > 8 else "")
    else:
        year_sql = year_expr(*year) if year else "CAST(NULL AS STRING)"
        sql = f"""
        SELECT
          {indicator_sql} AS indicator,
          {country_sql} AS country,
          {year_sql} AS year,
          {source_sql} AS source,
          COUNT(*) AS n
        FROM {full}
        GROUP BY 1, 2, 3, 4
        """
        year_mode = "column" if year else "none"
        year_column = year[0] if year else ""

    meta = {
        "year_mode": year_mode,
        "indicator_column": indicator[0] if indicator else "",
        "country_column": country[0] if country else "",
        "year_column": year_column,
        "source_column": ",".join(name for name, _data_type in sources),
        "source_is_fallback": source_fallback,
        "indicator_unmapped": indicator is None,
        "country_unmapped": country is None,
        "year_unmapped": year is None and not wide,
        "source_unmapped": not sources,
    }
    return sql, meta


def _empty_summary(dataset: str, table: str, meta: dict, error: str = "") -> dict:
    return {
        "dataset": dataset,
        "table": table,
        "datapoints": 0,
        "year_mode": meta.get("year_mode", ""),
        "indicator_column": meta.get("indicator_column", ""),
        "country_column": meta.get("country_column", ""),
        "year_column": meta.get("year_column", ""),
        "source_column": meta.get("source_column", ""),
        "source_is_fallback": meta.get("source_is_fallback", False),
        "indicator_unmapped": meta.get("indicator_unmapped", True),
        "country_unmapped": meta.get("country_unmapped", True),
        "year_unmapped": meta.get("year_unmapped", True),
        "source_unmapped": meta.get("source_unmapped", True),
        "indicator_null": 0,
        "country_null": 0,
        "year_null": 0,
        "source_null": 0,
        "error": error,
    }


def count_table(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    columns: list[tuple[str, str]],
    rules: dict,
) -> tuple[list[dict], dict]:
    sql, meta = build_sql(project, dataset, table, columns, rules)
    summary = _empty_summary(dataset, table, meta)
    rows: list[dict] = []
    try:
        for record in client.query(sql).result():
            n = int(record.n)
            item = {
                "dataset": dataset,
                "table": table,
                "indicator": record.indicator,
                "country": record.country,
                "year": record.year,
                "source": record.source,
                "source_is_fallback": meta["source_is_fallback"],
                "n": n,
            }
            rows.append(item)
            summary["datapoints"] += n
            if record.indicator is None:
                summary["indicator_null"] += n
            if record.country is None:
                summary["country_null"] += n
            if record.year is None:
                summary["year_null"] += n
            if record.source is None:
                summary["source_null"] += n
    except Exception as exc:
        summary["error"] = str(exc).replace("\n", " ")[:400]
    return rows, summary


def main() -> int:
    _load_dotenv()
    parser = argparse.ArgumentParser(description="Count landing datapoints by indicator, country, year, and source")
    parser.add_argument("--datasets", default="", help="Comma-separated dataset ids (default: every landing* dataset)")
    parser.add_argument("--match", default="", help="Only tables whose id matches this regular expression")
    parser.add_argument("--max-tables", type=int, default=0, help="Stop after this many tables (0 = all)")
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()

    project = (os.environ.get("BQ_PROJECT") or "").strip()
    if not project:
        print("Set BQ_PROJECT (e.g. in data/local/.env)", file=sys.stderr)
        return 1

    rules = load_map()
    client = bigquery.Client(project=project)
    datasets = landing_datasets(client, project)
    if args.datasets.strip():
        wanted = {part.strip() for part in args.datasets.split(",") if part.strip()}
        datasets = [dataset for dataset in datasets if dataset in wanted]
    if not datasets:
        print(f"No landing datasets selected in {project}", file=sys.stderr)
        return 1

    work: list[tuple[str, str, list[tuple[str, str]]]] = []
    for dataset in datasets:
        columns_by_table = table_columns(client, project, dataset)
        tables = base_tables(client, project, dataset)
        for table in tables:
            if args.match and not re.search(args.match, table):
                continue
            work.append((dataset, table, columns_by_table.get(table, [])))
            if args.max_tables and len(work) >= args.max_tables:
                break
        if args.max_tables and len(work) >= args.max_tables:
            break

    print(f"Counting {len(work)} base tables in {len(datasets)} datasets")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    lock = threading.Lock()
    done = 0
    failed = 0

    with COUNTS_PATH.open("w", encoding="utf-8", newline="") as counts_file, SUMMARY_PATH.open(
        "w", encoding="utf-8", newline=""
    ) as summary_file:
        counts_writer = csv.DictWriter(counts_file, fieldnames=COUNT_FIELDS)
        summary_writer = csv.DictWriter(summary_file, fieldnames=SUMMARY_FIELDS)
        counts_writer.writeheader()
        summary_writer.writeheader()

        def write_result(rows: list[dict], summary: dict) -> None:
            nonlocal done, failed
            with lock:
                for row in rows:
                    counts_writer.writerow(row)
                summary_writer.writerow(summary)
                counts_file.flush()
                summary_file.flush()
                done += 1
                if summary.get("error"):
                    failed += 1
                if done % 25 == 0 or done == len(work):
                    print(f"  {done}/{len(work)} tables, {failed} failed")

        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = [
                pool.submit(count_table, client, project, dataset, table, columns, rules)
                for dataset, table, columns in work
            ]
            for future in as_completed(futures):
                rows, summary = future.result()
                write_result(rows, summary)

    print(f"Wrote {COUNTS_PATH}")
    print(f"Wrote {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

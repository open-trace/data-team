#!/usr/bin/env python3
"""
Restore BigQuery bronze and silver tables using time travel only.

There are no separate snapshots; this script uses BigQuery's built-in time travel
(FOR SYSTEM_TIME AS OF) to restore table data from a point in the past (within
the time-travel window, typically 7 days).

Reads the list of tables from dbt/models/sources.yml (bronze and silver only).
For each table, runs:
  CREATE OR REPLACE TABLE `project.dataset.table` AS
  SELECT * FROM `project.dataset.table` FOR SYSTEM_TIME AS OF TIMESTAMP("...")

Works for existing tables (overwrites with older data) and recently-deleted
tables (recreates from history).

Usage (from repo root):
  # Restore to state from 2 days ago (default)
  python data/local/scripts/bq_snapshot_restore.py

  # Restore to a specific timestamp (ISO 8601)
  python data/local/scripts/bq_snapshot_restore.py --timestamp "2025-02-12T12:00:00Z"

  # Restore to N days ago
  python data/local/scripts/bq_snapshot_restore.py --days-ago 1

  # Dry run: only print what would be restored
  python data/local/scripts/bq_snapshot_restore.py --days-ago 2 --dry-run

  # Restore only bronze or only silver
  python data/local/scripts/bq_snapshot_restore.py --layer bronze

  # Skip tables that didn't exist at that time (default: skip and continue)
  python data/local/scripts/bq_snapshot_restore.py --no-skip-missing  # fail on missing

If all tables report "not found": the time-travel point may be before the tables
existed, or the tables never existed in BQ. Try a more recent --days-ago or
--timestamp. Time-travel window is typically 7 days.

Requires: google-cloud-bigquery, PyYAML. Set BQ_PROJECT, BQ_DATASET_* in data/local/.env.
Auth: GOOGLE_APPLICATION_CREDENTIALS or gcloud auth.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SOURCES_YML = REPO_ROOT / "dbt" / "models" / "sources.yml"


def _load_dotenv() -> None:
    env_file = REPO_ROOT / "data" / "local" / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().replace("export ", "", 1).strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


def get_tables_from_sources_yml() -> dict[str, list[str]]:
    """Return {'bronze': ['table1', ...], 'silver': ['table2', ...]} from sources.yml."""
    try:
        import yaml
    except ImportError:
        print("Install PyYAML: pip install pyyaml", file=sys.stderr)
        sys.exit(1)

    if not SOURCES_YML.exists():
        print(f"Missing {SOURCES_YML.relative_to(REPO_ROOT)}", file=sys.stderr)
        sys.exit(1)

    data = yaml.safe_load(SOURCES_YML.read_text(encoding="utf-8"))
    out: dict[str, list[str]] = {"bronze": [], "silver": []}
    for source in data.get("sources", []):
        name = source.get("name")
        if name not in ("bronze", "silver"):
            continue
        for t in source.get("tables", []):
            table_name = t.get("name") if isinstance(t, dict) else None
            if table_name:
                out[name].append(table_name)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Restore BQ bronze/silver tables using time travel only (no separate snapshots)"
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        default=None,
        help='Time-travel point in ISO 8601 (e.g. "2025-02-12T12:00:00Z"). Overrides --days-ago.',
    )
    parser.add_argument(
        "--days-ago",
        type=float,
        default=2.0,
        help="Time-travel point as N days before now (default: 2)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print tables and timestamp; do not run restore",
    )
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "both"],
        default="both",
        help="Which layer(s) to restore (default: both)",
    )
    parser.add_argument(
        "--no-skip-missing",
        action="store_true",
        help="Fail when a table did not exist at snapshot; default is to skip and continue",
    )
    args = parser.parse_args()

    _load_dotenv()
    project = os.environ.get("BQ_PROJECT", "").strip()
    if not project:
        print("Set BQ_PROJECT (e.g. in data/local/.env)", file=sys.stderr)
        sys.exit(1)
    dataset_bronze = os.environ.get("BQ_DATASET_BRONZE", "bronze")
    dataset_silver = os.environ.get("BQ_DATASET_SILVER", "silver")

    if args.timestamp:
        snapshot_ts = args.timestamp
    else:
        t = datetime.now(timezone.utc) - timedelta(days=args.days_ago)
        snapshot_ts = t.strftime("%Y-%m-%dT%H:%M:%SZ")

    tables_by_layer = get_tables_from_sources_yml()
    to_restore: list[tuple[str, str]] = []
    if args.layer in ("bronze", "both"):
        to_restore.extend((dataset_bronze, tbl) for tbl in tables_by_layer["bronze"])
    if args.layer in ("silver", "both"):
        to_restore.extend((dataset_silver, tbl) for tbl in tables_by_layer["silver"])

    if not to_restore:
        print("No bronze/silver tables found in sources.yml", file=sys.stderr)
        sys.exit(1)

    print(f"Time-travel point: {snapshot_ts}")
    if not args.timestamp and args.days_ago > 7:
        print("(Warning: point > 7 days ago may be outside BigQuery time-travel window.)")
    print(f"Tables to restore: {len(to_restore)}")
    for dataset, table in to_restore:
        print(f"  - {dataset}.{table}")

    if args.dry_run:
        print("Dry run: no restore performed.")
        return

    try:
        from google.cloud import bigquery
    except ImportError:
        print("Install: pip install google-cloud-bigquery", file=sys.stderr)
        sys.exit(1)

    client = bigquery.Client(project=project)
    skip_missing = not args.no_skip_missing
    ok = 0
    skipped = 0
    err = 0
    for dataset, table in to_restore:
        full = f"`{project}`.`{dataset}`.`{table}`"
        sql = (
            f"CREATE OR REPLACE TABLE {full} AS\n"
            f"SELECT * FROM {full}\n"
            f"FOR SYSTEM_TIME AS OF TIMESTAMP({repr(snapshot_ts)})"
        )
        try:
            client.query(sql).result()
            print(f"OK   {dataset}.{table}")
            ok += 1
        except Exception as e:
            msg = str(e).lower()
            is_not_found = "404" in msg or "not found" in msg
            if is_not_found and skip_missing:
                print(f"SKIP {dataset}.{table} (table did not exist at time-travel point)")
                skipped += 1
            else:
                print(f"FAIL {dataset}.{table}: {e}", file=sys.stderr)
                err += 1

    print(f"\nDone: {ok} restored, {skipped} skipped (not at time-travel point), {err} failed.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Copy a capped partition of every base table in BigQuery datasets whose id
contains "landing" into the local database.

Each dataset becomes a Postgres schema of the same name. Row cap is
BQ_PARTITION_LIMIT (default 10000). Views are skipped.

Usage (from data-eng/):
  python data/local/scripts/sync_landing_partitions.py
  python data/local/scripts/sync_landing_partitions.py --limit 1000
  python data/local/scripts/sync_landing_partitions.py --list-only
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from bq_partition_to_local import get_config, sync_table  # noqa: E402
from bq_table_lists import local_table_name  # noqa: E402


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


def landing_datasets(client, project: str) -> list[str]:
    found: list[str] = []
    for ds in client.list_datasets(project=project):
        dataset_id = str(getattr(ds, "dataset_id", "") or "")
        if dataset_id and "landing" in dataset_id.lower():
            found.append(dataset_id)
    return sorted(found)


def base_tables(client, project: str, dataset_id: str) -> list[str]:
    tables: list[str] = []
    for item in client.list_tables(DatasetReference(project, dataset_id)):
        table_type = str(getattr(item, "table_type", "") or "")
        if table_type != "TABLE":
            continue
        table_id = str(getattr(item, "table_id", "") or "")
        if table_id:
            tables.append(table_id)
    return sorted(tables)


def main() -> int:
    _load_dotenv()

    parser = argparse.ArgumentParser(
        description="Sync a capped partition of every base table in landing* BigQuery datasets"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.environ.get("BQ_PARTITION_LIMIT", "10000")),
        help="Max rows per table (default: BQ_PARTITION_LIMIT or 10000)",
    )
    parser.add_argument(
        "--datasets",
        default="",
        help="Comma-separated dataset ids to copy (default: every id containing landing)",
    )
    parser.add_argument(
        "--match",
        default="",
        help="Only copy tables whose id matches this regular expression",
    )
    parser.add_argument(
        "--append-index",
        action="store_true",
        help="Append to the partition index instead of replacing it",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Print datasets and base tables without copying",
    )
    args = parser.parse_args()

    project = (os.environ.get("BQ_PROJECT") or "").strip()
    if not project:
        print("Set BQ_PROJECT (e.g. in data/local/.env)", file=sys.stderr)
        return 1

    client = bigquery.Client(project=project)
    datasets = landing_datasets(client, project)
    if args.datasets.strip():
        wanted = {d.strip() for d in args.datasets.split(",") if d.strip()}
        datasets = [d for d in datasets if d in wanted]
    if not datasets:
        print(f"No datasets containing 'landing' in project {project}", file=sys.stderr)
        return 1

    plan: list[tuple[str, str]] = []
    for dataset_id in datasets:
        for table_id in base_tables(client, project, dataset_id):
            if args.match and not re.search(args.match, table_id):
                continue
            plan.append((dataset_id, table_id))

    print(f"Project {project}: {len(datasets)} landing datasets, {len(plan)} base tables")
    for dataset_id in datasets:
        n = sum(1 for ds, _ in plan if ds == dataset_id)
        print(f"  {dataset_id}: {n} tables")

    if args.list_only:
        for dataset_id, table_id in plan:
            print(f"{dataset_id}.{table_id}")
        return 0

    # A leftover partition filter would drop rows from tables that lack that column.
    os.environ.pop("BQ_PARTITION_FILTER", None)
    base = get_config()
    base["bq_project"] = project
    base["partition_filter"] = ""
    base["limit"] = args.limit
    if not (base.get("local_db_url") or "").strip():
        print("Set LOCAL_DB_URL (e.g. in data/local/.env)", file=sys.stderr)
        return 1

    index_path = REPO_ROOT / "data" / "local" / "out" / "landing_partition_index.csv"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    failed: list[str] = []
    index_mode = "a" if args.append_index and index_path.exists() else "w"
    with index_path.open(index_mode, encoding="utf-8", newline="") as index_file:
        if index_mode == "w":
            index_file.write("dataset,bq_table,local_schema,local_table,rows,error\n")
        for i, (dataset_id, table_id) in enumerate(plan, start=1):
            local_name = local_table_name(table_id)
            print(f"[{i}/{len(plan)}] Syncing {dataset_id}.{table_id} ...")
            cfg = dict(base)
            cfg["bq_dataset"] = dataset_id
            cfg["bq_table"] = table_id
            cfg["target_schema"] = dataset_id
            cfg["target_table"] = local_name
            error = ""
            rows = 0
            try:
                rows = sync_table(cfg, client=client)
            except Exception as exc:
                error = str(exc).replace("\n", " ")[:300]
                failed.append(f"{dataset_id}.{table_id}")
                print(f"Warning: {dataset_id}.{table_id} failed: {error}", file=sys.stderr)
            index_file.write(
                f"{dataset_id},{_csv(table_id)},{dataset_id},{_csv(local_name)},{rows},{_csv(error)}\n"
            )
            index_file.flush()

    print(f"Done. {len(plan) - len(failed)} synced, {len(failed)} failed.")
    print(f"Index: {index_path}")
    if failed:
        for name in failed:
            print(f"  failed: {name}", file=sys.stderr)
    return 0


def _csv(value: str) -> str:
    text = str(value)
    if any(ch in text for ch in ",\"\n"):
        return '"' + text.replace('"', '""') + '"'
    return text


if __name__ == "__main__":
    raise SystemExit(main())

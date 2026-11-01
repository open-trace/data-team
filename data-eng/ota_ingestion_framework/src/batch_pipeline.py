# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Prepare source references, assess access, and collect supported data in one run.

Run: python -m src.batch_pipeline --workers 4 --max-seconds 30
Recent matching assessments are reused for 24 hours. --recheck refreshes all.
No BigQuery writes. Unsupported sources are skipped, not reverse engineered.
"""

import argparse
import csv
import hashlib
import json
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlsplit

from src.prepare_sources import main as prepare
from src.source_tester import ASSESSMENT_VERSION, assess, positive_seconds, save_result
from src.batch_review import worker_count, write_review, write_review_workbook, review_row
from src.collector import fetch_source
from src.common.logging import get_logger
from src.common.source_config import GENERATED, ROOT, DATA, load_candidates, load_testing_rules, source_folder
from src.common.source_testing import ProbeError
from src.common.web_collection import NeedsAdapter

logger = get_logger(__name__)


def cached_assessment(source, rules):
    folder = source_folder(source["source_id"])
    try:
        profile = json.loads((folder / "profile.json").read_text())
        assessment = json.loads((folder / "assessment.json").read_text())
        checked = datetime.fromisoformat(assessment["checked_at_utc"])
        age = (datetime.now(timezone.utc) - checked).total_seconds()
        if (profile.get('assessment_version') == ASSESSMENT_VERSION
                and 0 <= age < 86400 and profile.get("test_status") != "IN_PROGRESS"
                and profile.get("checked_at_utc") == assessment["checked_at_utc"]
                and profile["source_url"] == source["source_url"]
                and assessment["source_url"] == source["source_url"]
                and all(profile["profile"].get(key) == value for key, value in rules.items())):
            return {**profile, **assessment}
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=worker_count, default=4, help="Concurrent hosts, default 4, maximum 8")
    parser.add_argument("--max-seconds", type=positive_seconds, default=30, help="Budget per source assessment/collection")
    parser.add_argument("--timeout", type=positive_seconds, default=6, help="Tester read timeout")
    parser.add_argument("--recheck", action="store_true", help="Ignore cached assessments and retest URLs")
    args = parser.parse_args(argv)
    preparation_exit = prepare([])
    sources, input_path = load_candidates()
    rules = load_testing_rules()
    input_hash = hashlib.sha256(input_path.read_bytes()).hexdigest()
    groups = defaultdict(list)
    for source in sources:
        groups[urlsplit(source["source_url"]).hostname].append(source)
    started = datetime.now(timezone.utc).isoformat()
    results, collections, lock = [], [], threading.Lock()

    def run_host(group):
        for source in group:
            result = None if args.recheck else cached_assessment(source, rules.get(source["source_id"], {}))
            reused = result is not None
            if result is None:
                save_result({**source, "connection_status": "NOT_TESTED", "test_status": "IN_PROGRESS",
                             "profile": rules.get(source["source_id"], {}), "enabled_for_ingestion": False}, input_path, input_hash)
                result = assess(source, rules.get(source["source_id"], {}), args)
                save_result(result, input_path, input_hash)
            profile = result["profile"]
            item = {"source_id": source["source_id"], "url": source["source_url"],
                    "assessment_reused": reused, "status": "SKIPPED", "reason": "", "csv_files": [], "rows_exported": 0}
            folder = DATA / "collected" / source["source_id"]
            before = set(folder.glob("*/*.csv"))
            if rules.get(source['source_id'], {}).get('exclude_reason'):
                item.update(status='EXCLUDED', reason=rules[source['source_id']]['exclude_reason'])
            elif result['connection_status'] != 'PASS':
                item.update(status='ACCESS_FAILED', reason=result.get('error') or 'Initial resource access not verified.')
            else:
                try:
                    complete = fetch_source(result, max_seconds=args.max_seconds, max_products=None)
                    item['status'] = 'COLLECTED' if complete else 'PARTIAL'
                except NeedsAdapter as error:
                    item.update(status='NEEDS_ADAPTER', reason=str(error))
                except Exception as error:
                    item.update(status='FAILED', reason=str(error) if isinstance(error, ProbeError) else type(error).__name__)
            for path in sorted(set(folder.glob("*/*.csv")) - before):
                with path.open(encoding="utf-8", newline="") as handle:
                    rows = max(0, sum(1 for _ in csv.reader(handle)) - 1)
                item["csv_files"].append({"path": str(path), "rows": rows})
                item["rows_exported"] += rows
            if item["status"] == "FAILED" and item["rows_exported"]:
                item["status"] = "PARTIAL"
            with lock:
                results.append(result)
                collections.append(item)
                (GENERATED / "batch_collection.json").write_text(json.dumps({
                    "started_at_utc": started, "status": "IN_PROGRESS", "sources": collections,
                }, indent=2) + "\n")
                logger.info("%s/%s %s: %s (%s CSV files, %s rows)", len(collections), len(sources),
                            source["source_id"], item["status"], len(item["csv_files"]), item["rows_exported"])

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for future in as_completed([pool.submit(run_host, group) for group in groups.values()]):
            future.result()
    write_review(results)
    write_review_workbook(results)
    (GENERATED / "batch_review_report.json").write_text(json.dumps({
        "assembled_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_file": str(input_path), "input_sha256": input_hash,
        "sources_reviewed": len(results),
        "assessments_reused": sum(item["assessment_reused"] for item in collections),
        "access_counts": dict(Counter(review_row(result)["access_result"] for result in results)),
        "scope": "Current inventory review, with matching assessments reused for up to 24 hours unless --recheck. Record access and collection are separate from production approval.",
    }, indent=2) + "\n")
    report = {"started_at_utc": started, "finished_at_utc": datetime.now(timezone.utc).isoformat(),
              "status": "FINISHED", "preparation_exit_code": preparation_exit,
              "input_file": str(input_path), "input_sha256": input_hash,
              "counts": dict(Counter(item["status"] for item in collections)),
              "csv_files_created": sum(len(item["csv_files"]) for item in collections),
              "raw_rows_exported": sum(item["rows_exported"] for item in collections),
              "scope": "Supported raw exports only. Rows include raw workbook grids and may overlap across URLs. No claim of unique price observations, complete pagination or reporting eligibility.",
              "sources": sorted(collections, key=lambda item: item["source_id"])}
    (GENERATED / "batch_collection.json").write_text(json.dumps(report, indent=2) + "\n")
    with (GENERATED / "batch_collection.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["source_id", "status", "csv_files", "rows_exported", "reason"])
        writer.writeheader()
        writer.writerows({**{key: item[key] for key in ["source_id", "status", "rows_exported", "reason"]},
                         "csv_files": len(item["csv_files"])} for item in report["sources"])
    logger.info("Finished: %s; %s new CSV files, %s raw rows. See %s", report["counts"],
                report["csv_files_created"], report["raw_rows_exported"], GENERATED / "batch_collection.json")
    return 1 if preparation_exit or any(item["status"] not in {"COLLECTED", "EXCLUDED"} for item in collections) else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        logger.error("Batch pipeline could not finish: %s", error)
        raise SystemExit(1)

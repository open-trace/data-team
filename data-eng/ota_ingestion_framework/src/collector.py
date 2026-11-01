# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Collect Tshwane statistics, Durban tables or configured JSON records locally."""

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from datetime import datetime, timezone

from src.common.logging import get_logger
from src.common.source_config import ROOT, DATA, load_sources, source_folder, load_testing_rules
from src.common.fetching_method import fetch_tshwane_prices, fetch_durban_records, fetch_cape_town_records
from src.common.source_testing import Client, Evidence, ProbeError, display_url, tls_bundle, observation_date
from src.common.price_output import PRICE_COLUMNS, export_prices_csv
from src.common.record_output import write_source_records
from src.common.tabular_files import tabular_sheets
from src.common.api_collection import collect_api_source
from src.common.web_collection import collect_web_source

logger = get_logger(__name__)


def collect_tshwane(source, *, max_seconds, max_products):
    """Save real source observations locally, with bounded coverage reporting."""
    source_id = source["source_id"]
    source_folder(source_id)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    folder = DATA / "raw" / source_id / run_id
    records_folder = DATA / "collected" / source_id / run_id
    folder.mkdir(parents=True, exist_ok=False)
    records_folder.mkdir(parents=True, exist_ok=False)
    suffix = f"{source_id}_{run_id}"
    jsonl_path = records_folder / f"prices_{suffix}.jsonl"
    csv_path = records_folder / f"prices_{suffix}.csv"
    conflicts_path = records_folder / f"conflicts_{suffix}.jsonl"
    report_path = folder / f"collection_report_{suffix}.json"

    def response_path(name):
        original = Path(name)
        return folder / f"{original.stem}_{suffix}{original.suffix}"

    evidence = Evidence(source)
    report = {
        "source_id": source_id, "source_url": source["source_url"], "run_id": run_id,
        "scope": "Daily statistics for all selectable products/packages returned by an unfiltered search; no historical backfill.",
        "started_at_utc": datetime.now(timezone.utc).isoformat(), "status": "IN_PROGRESS",
        "completeness": "UNKNOWN", "returned_listing_coverage": "UNKNOWN",
        "products_discovered": 0, "products_attempted": 0, "products_finished": 0,
        "packages_discovered": 0, "packages_attempted": 0, "packages_fetched": 0,
        "records_saved": 0, "duplicates_skipped": 0, "conflicting_records": 0,
        "quality_issue_records": 0, "observation_dates": [], "failures": [],
        "pagination_gaps": [], "responses": [], "limits": {"max_seconds": max_seconds, "max_products": max_products},
        "records_file": str(jsonl_path), "csv_file": str(csv_path),
        "conflicts_file": str(conflicts_path), "csv_columns": PRICE_COLUMNS, "csv_schema_version": 1,
    }
    freshness = source["profile"].get("freshness_days")
    dates, seen = set(), {}

    def save_response(name, body):
        path = response_path(name)
        with path.open("xb") as handle:
            handle.write(body)
        report["responses"].append({"file": path.name, "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body), "retrieved_at_utc": datetime.now(timezone.utc).isoformat()})

    try:
        with jsonl_path.open("x", encoding="utf-8") as rows, conflicts_path.open("x", encoding="utf-8") as conflicts:
            with tls_bundle(source["profile"]) as bundle:
                client = Client(evidence, verify=bundle, timeout=15, max_seconds=max_seconds)
                try:
                    for event in fetch_tshwane_prices(client, source["source_url"], save_response, source_id=source_id, max_products=max_products):
                        kind = event["kind"]
                        if kind == "catalogue":
                            report["products_discovered"] = len(event["products"])
                            if event["pagination"]:
                                report["pagination_gaps"].append({"stage": "products", "actions": event["pagination"]})
                            with response_path("products.csv").open("x", encoding="utf-8", newline="") as handle:
                                writer = csv.DictWriter(handle, fieldnames=["product_name_source", "event_target", "event_argument"])
                                writer.writeheader()
                                writer.writerows(event["products"])
                            logger.info("Discovered %s products; starting package/statistics collection.", report["products_discovered"])
                        elif kind == "product_started":
                            report["products_attempted"] += 1
                            logger.info("Product %s/%s: %s", report["products_attempted"], report["products_discovered"], event["product"])
                        elif kind == "packages":
                            report["packages_discovered"] += event["count"]
                            if event["pagination"]:
                                report["pagination_gaps"].append({"stage": "packages", "product": event["product"], "actions": event["pagination"]})
                        elif kind == "package_started":
                            report["packages_attempted"] += 1
                        elif kind == "product_finished":
                            report["products_finished"] += 1
                        elif kind == "failure":
                            report["failures"].append(event)
                            logger.warning("Failed %s for %s: %s", event["stage"], event["product"], event["error"])
                        elif kind == "stopped":
                            report["stopped"] = event
                            logger.warning("Collection stopped: %s", event["reason"])
                        elif kind == "record":
                            report["packages_fetched"] += 1
                            record = event["record"]
                            record["run_id"] = run_id
                            record["response_file"] = str(response_path(record["response_file"]))
                            if freshness is not None and record["age_days"] > freshness:
                                record["quality_issues"].append(f"Stale observation: older than {freshness} days.")
                            record["quality_status"] = "REVIEW" if record["quality_issues"] else "PASS"
                            dates.add(record["observed_at"])
                            comparable = {k: v for k, v in record.items() if k not in {"retrieved_at_utc", "response_file"}}
                            signature = hashlib.sha256(json.dumps(comparable, sort_keys=True).encode()).hexdigest()
                            identity = record["observation_id"]
                            if identity in seen:
                                if seen[identity] == signature:
                                    report["duplicates_skipped"] += 1
                                else:
                                    report["conflicting_records"] += 1
                                    record["quality_issues"].append("Conflicting values for the same observation identity in this run.")
                                    record["quality_status"] = "REVIEW"
                                    conflicts.write(json.dumps(record, ensure_ascii=False) + "\n")
                                    conflicts.flush()
                                continue
                            seen[identity] = signature
                            rows.write(json.dumps(record, ensure_ascii=False) + "\n")
                            rows.flush()
                            report["records_saved"] += 1
                            report["quality_issue_records"] += bool(record["quality_issues"])
                            logger.info("Saved record %s, date %s", report["records_saved"], record["observed_at"])
                finally:
                    client.close()
        all_returned = bool(report["products_discovered"]) and report["products_finished"] == report["products_discovered"] and not (report["failures"] or report["pagination_gaps"] or report.get("stopped")) and report["packages_fetched"] == report["packages_discovered"]
        report["returned_listing_coverage"] = "COMPLETE" if all_returned else "PARTIAL"
        report["completeness"] = "UNKNOWN" if all_returned else "PARTIAL"
        report["status"] = "FETCHED_RETURNED_LISTING" if all_returned and not (report["quality_issue_records"] or report["conflicting_records"]) else "PARTIAL"
        report["completeness_reason"] = "Coverage is measured against returned selections; no authoritative total market catalogue or historical coverage has been established."
    except BaseException as error:
        report["status"] = "PARTIAL" if report["records_saved"] else "FAIL"
        report["completeness"] = "PARTIAL"
        report["error"] = str(error) if isinstance(error, ProbeError) else type(error).__name__
        raise
    finally:
        # Export the actual saved records, including partial runs; no second fetch.
        if jsonl_path.exists():
            try:
                report["csv_records"] = export_prices_csv(jsonl_path, csv_path)
                report["csv_exported_at_utc"] = datetime.now(timezone.utc).isoformat()
                if report["csv_records"] != report["records_saved"]:
                    raise ValueError("CSV count differs from the saved-record count.")
                logger.info("CSV: %s (%s records)", csv_path, report["csv_records"])
            except Exception as error:
                report["status"] = "FAIL"
                report["csv_export_error"] = type(error).__name__
                logger.error("CSV export failed (%s); see collection report.", type(error).__name__)
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["observation_dates"] = sorted(dates)
        report["products_unvisited"] = report["products_discovered"] - report["products_attempted"]
        report["known_packages_unvisited"] = report["packages_discovered"] - report["packages_attempted"]
        report["request_evidence"] = evidence.report
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        logger.info("Saved %s records; dates: %s; status: %s; completeness: %s", report["records_saved"], ", ".join(report["observation_dates"]) or "none", report["status"], report["completeness"])
        logger.info("Records: %s", report["records_file"])
        logger.info("Collection report: %s", report_path)
    return report["status"] == "FETCHED_RETURNED_LISTING"

def collect_records_source(source, *, max_seconds):
    """Save a supported HTML daily table with source-shaped values."""
    source_id = source["source_id"]
    source_folder(source_id)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    suffix = f"{source_id}_{run_id}"
    is_durban = source["profile"].get("probe") == "durban"
    is_cape_town = source["profile"].get("probe") == "cape_town"
    is_html = is_durban or is_cape_town
    raw = DATA / "raw" / source_id / run_id
    collected = DATA / "collected" / source_id / run_id
    raw.mkdir(parents=True, exist_ok=False)
    collected.mkdir(parents=True, exist_ok=False)
    response_path = raw / f"response_{suffix}.{'html' if is_html else 'json'}"
    report_path = raw / f"collection_report_{suffix}.json"
    jsonl_path = collected / f"records_{suffix}.jsonl"
    csv_path = collected / f"records_{suffix}.csv"
    profile = source["profile"]
    evidence = Evidence(source)
    report = {
        "source_id": source_id, "source_url": source["source_url"], "run_id": run_id,
        "started_at_utc": datetime.now(timezone.utc).isoformat(), "status": "IN_PROGRESS",
        "validation_scope": "structure", "completeness": "UNKNOWN",
        "scope": profile.get("scope", "One JSON response; source-wide coverage unverified."),
        "records_path": profile.get("records_path", ""), "records_saved": 0,
        "response_file": str(response_path), "records_file": str(jsonl_path), "csv_file": str(csv_path),
        "price_semantics_and_freshness": "NOT_TESTED",
        "pagination_and_coverage": "NOT_TESTED", "response_saved": False,
        "csv_null_semantics": "Missing keys and explicit null become blank; original JSONL and response retain the distinction.",
        "tester_status": source.get("test_status", "NOT_TESTED"),
        "tester_assessment_file": str(source_folder(source_id) / "assessment.json"),
        "production_ready": False,
    }
    if is_durban:
        report.update(scope="Initial Durban daily HTML table only; no commodity filters, pagination or historical backfill.",
                      validation_scope="table_structure", records_path="HTML daily price table",
                      market="Durban Fresh Produce Market",
                      source_values_policy="Keep all parsed row values, including duplicates and inconsistent prices; transformations decide reporting eligibility.")
    if is_cape_town:
        report.update(scope="Initial Cape Town daily table only; coverage unverified.",
                      validation_scope="table_structure", records_path="HTML daily price table", market="Cape Town Market")
    assessment_path = source_folder(source_id) / "assessment.json"
    if assessment_path.exists():
        assessment = json.loads(assessment_path.read_text(encoding="utf-8"))
        report["tester_checks_requiring_review"] = [
            check for check in assessment.get("evidence", {}).get("checks", [])
            if check["status"] in {"FAIL", "PARTIAL", "NOT_TESTED"}
        ]

    def save_response(body):
        with response_path.open("xb") as handle:
            handle.write(body)
        report.update(response_saved=True, response_bytes=len(body), response_sha256=hashlib.sha256(body).hexdigest(), retrieved_at_utc=datetime.now(timezone.utc).isoformat())

    try:
        with tls_bundle(profile) as ca_bundle:
            client = Client(evidence, verify=ca_bundle, timeout=10, max_seconds=max_seconds)
            try:
                if is_html:
                    fetch_table = fetch_durban_records if is_durban else fetch_cape_town_records
                    records, final_url, headers = fetch_table(client, source["source_url"], save_response=save_response)
                    report.update(final_url=display_url(final_url), source_column_labels=headers)
            finally:
                client.close()
        report["csv_columns"] = write_source_records(records, jsonl_path, csv_path)
        report["records_saved"] = report["csv_records"] = len(records)
        fingerprints = [hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest() for record in records]
        report["exact_duplicate_records_detected"] = len(records) - len(set(fingerprints))
        report["duplicates_policy"] = "Retain original source records; reporting deduplication belongs to transformation."
        if is_html:
            date_field = "date" if is_durban else "source_observation_date"
            dates = [observation_date(record.get(date_field)) for record in records]
            report["observation_dates"] = sorted({date.isoformat() for date in dates if date})
            report["unparsed_observation_dates"] = sum(date is None for date in dates)
            logger.info("Source observation dates: %s", ", ".join(report["observation_dates"]) or "unverified")
        report["response_complete"] = evidence.report["endpoints"][-1].get("complete", False)
        report["status"] = "FETCHED_RESPONSE" if records and report["response_complete"] else "PARTIAL"
        logger.info("Saved %s %s records with %s source columns; source values preserved.", len(records), "HTML table" if is_html else "API", len(report["csv_columns"]))
        if report["exact_duplicate_records_detected"]:
            logger.warning("Retained %s exact duplicate records for transformation review.", report["exact_duplicate_records_detected"])
        if source.get("test_status") != "PASS":
            logger.warning("Tester status is %s; collection is raw data access, not quality approval. See report for saved tester findings.", source.get("test_status"))
        logger.info("CSV: %s", csv_path)
        logger.info("JSONL: %s", jsonl_path)
        logger.info("Coverage: UNKNOWN; one response fetched, not a claim of all market/history coverage.")
    except BaseException as error:
        report["status"] = "FAIL"
        report["error"] = str(error) if isinstance(error, ProbeError) else type(error).__name__
        raise
    finally:
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["request_evidence"] = evidence.report
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        logger.info("Collection report: %s", report_path)
    return report["status"] == "FETCHED_RESPONSE"


def collect_file_source(source, *, max_seconds):
    """Export a file, or bounded published downloads from an HTML page."""
    from src.source_tester import inspect_response
    from src.common.source_access import download_links
    from zipfile import BadZipFile

    source_id = source["source_id"]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    suffix = f"{source_id}_{run_id}"
    raw = DATA / "raw" / source_id / run_id
    collected = DATA / "collected" / source_id / run_id
    raw.mkdir(parents=True, exist_ok=False)
    collected.mkdir(parents=True, exist_ok=False)
    report_path = raw / f"collection_report_{suffix}.json"
    evidence = Evidence(source)
    report = {"source_id": source_id, "source_url": source["source_url"], "run_id": run_id,
              "started_at_utc": datetime.now(timezone.utc).isoformat(), "status": "IN_PROGRESS",
              "completeness": "UNKNOWN", "records_saved": 0, "exports": [], "failures": [], "responses": [],
              "scope": "Raw file tables; at most 10 selections and 10000 rows per selection. Linked downloads are bounded; history and price coverage remain unverified.",
              "excel_policy": "Column letters and original row numbers; header rows retained; cached cell values read. Original workbook is authoritative.",
              "tester_status": source.get("test_status"), "production_ready": False}
    try:
        url = source["source_url"]
        if source["profile"].get("probe") == "nbs":
            assessment = json.loads((source_folder(source_id) / "assessment.json").read_text())
            endpoint = next((item for item in assessment["evidence"]["endpoints"]
                             if item["name"] == "first_listed_download" and item["status"] == "PASS"), None)
            if endpoint is None:
                raise ProbeError("No verified NBS download is available in the assessment.")
            url = endpoint["url"]
        report["resource_url"] = url
        with tls_bundle(source["profile"]) as bundle:
            download_timeout = source['profile'].get('download_timeout_seconds', 30)
            if (isinstance(download_timeout, bool) or not isinstance(download_timeout, (int, float))
                    or not math.isfinite(download_timeout) or not 1 <= download_timeout <= 120):
                raise ProbeError('download_timeout_seconds must be between 1 and 120.')
            report['download_timeout_seconds'] = download_timeout
            client = Client(evidence, verify=bundle, timeout=download_timeout, max_seconds=max_seconds, max_bytes=32_000_000)
            try:
                body, final_url = client.get("tabular_resource", url)
                endpoint = evidence.report["endpoints"][-1]
                kind = inspect_response(body, endpoint.get("content_type", ""), endpoint["complete"])["response_format"]
                if kind == 'html':
                    (raw / f'page_{suffix}.html').write_bytes(body)
                    candidates = download_links(body, final_url)
                    limit = source['profile'].get('download_limit', 3)
                    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 20:
                        raise ProbeError('download_limit must be an integer from 1 to 20.')
                    report.update(download_candidates=candidates, download_limit=limit,
                                  links_unvisited=max(0, len(candidates)-limit), page_url=final_url)
                    if not candidates:
                        raise ProbeError('No supported CSV/XLSX/ZIP download anchors found on the returned page.')
                    resources = [(item['url'], None, item['advertised_format']) for item in candidates[:limit]]
                else:
                    resources = [(final_url, body, kind)]
                for file_index, (resource_url, saved_body, advertised) in enumerate(resources):
                    try:
                        if saved_body is None:
                            saved_body, resource_final = client.get(f'published_file_{file_index:02d}', resource_url)
                        else:
                            resource_final = final_url
                        endpoint = evidence.report['endpoints'][-1]
                        kind = inspect_response(saved_body, endpoint.get('content_type', ''), endpoint['complete'])['response_format']
                        # Delimited-file URLs still undergo the CSV reader's header/row checks.
                        if kind == 'unknown' and advertised == 'csv':
                            kind = 'csv'
                        path = raw / f"response_{file_index:02d}_{suffix}.{kind if kind in {'csv', 'xlsx', 'zip'} else 'bin'}"
                        path.write_bytes(saved_body)
                        response = {'file': str(path), 'url': display_url(resource_final),
                                    'sha256': hashlib.sha256(saved_body).hexdigest(),
                                    'retrieved_at_utc': datetime.now(timezone.utc).isoformat()}
                        report['responses'].append(response)
                        if 'response_file' not in report:
                            report.update(response_file=str(path), response_sha256=response['sha256'],
                                          retrieved_at_utc=response['retrieved_at_utc'], final_url=response['url'])
                        if kind not in {'csv', 'xlsx', 'zip'}:
                            raise ProbeError('Published link did not return a supported tabular file; response retained.')
                        for label, records, truncated in tabular_sheets(saved_body, kind, deadline=client.deadline, byte_limit=32_000_000):
                            index = len(report['exports'])
                            if index >= 10:
                                report['selection_limit_reached'] = True
                                break
                            csv_path = collected / f"table_{index:02d}_{suffix}.csv"
                            jsonl_path = csv_path.with_suffix('.jsonl')
                            columns = write_source_records(records, jsonl_path, csv_path)
                            report['exports'].append({'label': label, 'csv_file': str(csv_path),
                                                      'records_file': str(jsonl_path), 'response_file': str(path),
                                                      'resource_url': response['url'], 'rows': len(records),
                                                      'columns': columns, 'row_limit_reached': truncated})
                            report['records_saved'] += len(records)
                            logger.info('Saved file table: %s (%s rows)', csv_path, len(records))
                        if report.get('selection_limit_reached'):
                            break
                    except (ProbeError, ValueError, OSError, KeyError, csv.Error, BadZipFile) as error:
                        report['failures'].append({'url': display_url(resource_url), 'error': str(error)})
                        logger.warning('File export incomplete for %s: %s', source_id, error)
            finally:
                client.close()
        limited = (report.get('selection_limit_reached') or report.get('links_unvisited')
                   or report['failures'] or any(item['row_limit_reached'] for item in report['exports']))
        report['status'] = 'FETCHED_RESPONSE' if report['records_saved'] and not limited else 'PARTIAL' if report['records_saved'] else 'FAIL'
    except BaseException as error:
        report['status'] = 'PARTIAL' if report['records_saved'] else 'FAIL'
        report['error'] = str(error) if isinstance(error, ProbeError) else type(error).__name__
        raise
    finally:
        report['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
        report['request_evidence'] = evidence.report
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
    if report['status'] == 'FAIL':
        raise ProbeError('No tabular records exported; inspect the saved collection report.')
    return report['status'] == 'FETCHED_RESPONSE'


def fetch_source(source, *, max_seconds, max_products):
    """Discover a supported catalogue or fetch the initial resource for another source."""
    profile = dict(source["profile"])
    # Collection contracts are independent of the tester's structural probe.
    # A source-specific contract must still validate each actual API response.
    configured = load_testing_rules().get(source["source_id"], {})
    if "collection" in configured:
        profile["collection"] = configured["collection"]
    source = {**source, "profile": profile}
    logger.info("Source: %s (%s)", source["display_name"], source["source_id"])
    logger.info("URL: %s", display_url(source["source_url"]))
    logger.info("Tester checks: %s (not production approval)", source.get("test_status", "unknown"))
    if profile.get("probe") == "tshwane":
        return collect_tshwane(source, max_seconds=max_seconds, max_products=max_products)
    if profile.get("probe") == "json_records" or (profile.get("response_format") == "json" and 'records_path' in profile):
        return collect_api_source(source, max_seconds=max_seconds)
    if profile.get("probe") in {"durban", "cape_town"}:
        return collect_records_source(source, max_seconds=max_seconds)
    if (profile.get("probe") == "nbs" or profile.get("response_format") in {"csv", "xlsx", "zip"}
             ):
        return collect_file_source(source, max_seconds=max_seconds)
    return collect_web_source(source, max_seconds=max_seconds)


def positive_seconds(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("Time budget must be finite and positive.")
    return number


def positive_count(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("Product limit must be positive.")
    return number



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-id", action="append", help="Repeat to select sources; omit to process all saved profiles")
    parser.add_argument("--list", action="store_true", help="List selected profiles without fetching")
    parser.add_argument("--max-seconds", type=positive_seconds, default=180, help="Per-source HTTP budget; partial progress is saved")
    parser.add_argument("--max-products", type=positive_count, help="Optional explicit pilot limit; omitted means all discovered products")
    args = parser.parse_args()
    sources = load_sources()
    if args.source_id:
        missing = set(args.source_id) - {s["source_id"] for s in sources}
        if missing:
            raise ValueError("Source IDs without saved profiles: " + ", ".join(sorted(missing)))
        sources = [s for s in sources if s["source_id"] in args.source_id]
    if not sources:
        raise ValueError("No source profiles selected. Run the tester first.")
    if args.list:
        for source in sources:
            print(source["source_id"], source.get("connection_status", "NOT_TESTED"), source.get("test_status", "NOT_TESTED"))
        return 0
    fetched, failed, skipped, duplicates, partial = 0, 0, 0, 0, 0
    seen = {}
    for source in sources:
        source_id = source["source_id"]
        if source.get("duplicate_of"):
            duplicates += 1
            logger.info("SKIP %s: duplicate of %s", source_id, source["duplicate_of"])
            continue
        if source.get("connection_status") != "PASS":
            skipped += 1
            logger.warning("SKIP %s: tester did not verify the initial connection; fix and retest.", source_id)
            continue
        key = source["source_url"]
        if key in seen:
            duplicates += 1
            logger.info("SKIP %s: same URL as %s", source_id, seen[key])
            continue
        seen[key] = source_id
        try:
            if fetch_source(source, max_seconds=args.max_seconds, max_products=args.max_products):
                fetched += 1
            else:
                partial += 1
        except Exception as error:
            # Source failures must not stop the remaining batch. Do not log response bodies.
            failed += 1
            detail = str(error) if isinstance(error, ProbeError) else type(error).__name__
            logger.error("FAIL %s: %s", source_id, detail)
    logger.info("Finished: %s source steps finished, %s partial, %s failed, %s unverified skipped, %s duplicates skipped.", fetched, partial, failed, skipped, duplicates)
    return 1 if failed or partial or skipped or not fetched else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        logger.error("Collector could not run: %s", error)
        raise SystemExit(1)

# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Test URLs and update one current profile/assessment per source; never ingest prices."""

import argparse
import csv
import hashlib
import io
import json
import math
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from src.common.logging import get_logger
from src.common.source_config import GENERATED, load_candidates, load_testing_rules, source_folder
from src.common.source_access import download_links
from src.common.source_probes import PROBES, discover
from src.common.source_testing import Client, Evidence, Page, ProbeError, display_url, tls_bundle

logger = get_logger(__name__)
ASSESSMENT_VERSION = 3


def positive_seconds(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("Time limits must be finite and positive.")
    return number


def inspect_response(body, content_type, complete):
    """Describe actual returned bytes, without guessing hidden endpoints or semantics."""
    result = {"response_format": "unknown", "fetch_method": "unresolved"}
    head = body.lstrip()[:512].lower()
    if body.startswith(b"%PDF-"):
        return {"response_format": "pdf", "fetch_method": "download", "format_validation": "signature_only"}
    if body.startswith(b"PK\x03\x04"):
        kind = "zip"
        if complete:
            with zipfile.ZipFile(io.BytesIO(body)) as archive:
                if "xl/workbook.xml" in archive.namelist():
                    kind = "xlsx"
        return {"response_format": kind, "fetch_method": "download", "format_validation": "container_identified" if complete else "partial_signature_only"}
    if "html" in content_type.lower() or b"<html" in head or b"<!doctype html" in head:
        page = Page(body.decode("utf-8-sig", errors="replace"))
        # Counts only: no cookies, hidden form values or source records in profiles.
        return {"response_format": "html", "fetch_method": "html", "table_count": len(page.tables), "form_count": len(page.forms), "link_count": len(page.links), "format_validation": "html_parsed"}
    if complete:
        try:
            value = json.loads(body)
        except (ValueError, UnicodeError):
            pass
        else:
            return {"response_format": "json", "fetch_method": "json", "json_root_type": type(value).__name__, "format_validation": "json_parsed"}
    if complete and ("csv" in content_type.lower() or "tab-separated" in content_type.lower()):
        text = body.decode("utf-8-sig")
        dialect = csv.Sniffer().sniff(text[:8192], delimiters=",;\t")
        reader = csv.reader(io.StringIO(text), dialect)
        first = next(reader, [])
        if len(first) > 1:
            return {"response_format": "csv", "fetch_method": "download", "delimiter": dialect.delimiter, "column_count": len(first), "format_validation": "delimited_text_parsed"}
    return result


def assess(source, rules, args):
    evidence = Evidence(source)
    entry = {"source_id": source["source_id"], "display_name": source["display_name"], "source_url": source["source_url"], "evidence_locations": source.get("evidence_locations", ""), "checked_at_utc": datetime.now(timezone.utc).isoformat(), "connection_status": "FAIL", "test_status": "FAIL", "profile": dict(rules), "enabled_for_ingestion": False, "error": None}
    entry['assessment_version'] = ASSESSMENT_VERSION
    client = None
    try:
        with tls_bundle(rules) as bundle:
            client = Client(evidence, verify=bundle, timeout=args.timeout, max_seconds=args.max_seconds, max_bytes=32_000_000)
            body, final_url = client.get("initial_resource", source["source_url"])
            endpoint = evidence.report["endpoints"][-1]
            entry["connection_status"] = "PASS"
            entry["profile"].update(inspect_response(body, endpoint.get("content_type", ""), endpoint["complete"]))
            entry["response_complete"] = endpoint["complete"]
            if entry['profile'].get('response_format') == 'html':
                links = download_links(body, final_url)
                entry['profile']['download_candidates'] = links
                if links:
                    entry['profile']['fetch_method'] = 'linked_downloads'
                    evidence.check('published_download_links', 'PASS',
                                   'Published file links found; their content and coverage remain unverified.',
                                   links_found=len(links))
            probe_name = rules.get("probe")
            if probe_name:
                if probe_name not in PROBES:
                    raise ProbeError("Configured source-specific check is not implemented.")
                PROBES[probe_name](client, evidence, source, rules)
            else:
                if entry["profile"].get("response_format") == "html":
                    discover(evidence, "initial_resource", Page(body.decode("utf-8-sig", errors="replace")), source["source_url"])
                evidence.check("data_records", "NOT_TESTED", "Response type identified only. Price semantics, observation dates, pagination, authentication and coverage require reviewed checks.")
            required = [c for c in evidence.report["checks"] if c.get("required", True)]
            bad = any(c["status"] == "FAIL" for c in required) or any(e["status"] != "PASS" for e in evidence.report["endpoints"])
            partial = any(c["status"] in {"NOT_TESTED", "PARTIAL"} for c in required) or any(not e.get("complete", False) for e in evidence.report["endpoints"])
            entry["test_status"] = "FAIL" if bad else ("PARTIAL" if partial else "PASS")
    except (ProbeError, OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile, csv.Error) as error:
        entry["error"] = str(error) if isinstance(error, ProbeError) else f"Inspection failed ({type(error).__name__})."
    except Exception as error:
        # Keep the rest of a batch running; never convert unexpected errors into success.
        entry["error"] = f"Unexpected tester error ({type(error).__name__})."
    finally:
        if client:
            client.close()
    entry["evidence"] = evidence.report
    return entry


def save_result(result, input_path, input_hash):
    """Replace current results, including failures, using atomic file replacements."""
    folder = source_folder(result["source_id"])
    folder.mkdir(parents=True, exist_ok=True)
    profile = {k: v for k, v in result.items() if k not in {"evidence", "error", "evidence_locations"}}
    profile["assessment_file"] = "assessment.json"
    profile["input_file"] = str(input_path)
    profile["input_sha256"] = input_hash
    assessment = {k: v for k, v in result.items() if k != "profile"}
    assessment.update(input_file=str(input_path), input_sha256=input_hash)
    for name, document in (("profile.json", profile), ("assessment.json", assessment)):
        path = folder / name
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="Candidate CSV; default: latest generated urls_*.csv")
    parser.add_argument("--source-id", action="append", help="Repeat to select candidates; default: all")
    parser.add_argument("--list", action="store_true", help="List candidates without network requests")
    parser.add_argument("--timeout", type=positive_seconds, default=8)
    parser.add_argument("--max-seconds", type=positive_seconds, default=30, help="Per-source HTTP budget")
    args = parser.parse_args()
    sources, input_path = load_candidates(args.input)
    if args.source_id:
        missing = set(args.source_id) - {s["source_id"] for s in sources}
        if missing:
            raise ValueError("Unknown source IDs: " + ", ".join(sorted(missing)))
        sources = [s for s in sources if s["source_id"] in args.source_id]
    if not sources:
        raise ValueError("No candidate sources selected.")
    if args.list:
        for source in sources:
            print(source["source_id"], display_url(source["source_url"]))
        return 0
    rules = load_testing_rules()
    started = datetime.now(timezone.utc)
    input_hash = hashlib.sha256(input_path.read_bytes()).hexdigest()
    for source in sources:
        source_folder(source["source_id"])  # Validate all directory names before requests.
    results, seen = [], {}
    for source in sources:
        key = source["source_url"]
        if key in seen:
            result = {
                "source_id": source["source_id"], "display_name": source["display_name"],
                "source_url": source["source_url"], "evidence_locations": source.get("evidence_locations", ""),
                "checked_at_utc": None, "connection_status": "NOT_TESTED",
                "test_status": "SKIPPED_DUPLICATE", "duplicate_of": seen[key],
                "profile": dict(rules.get(source["source_id"], {})),
                "enabled_for_ingestion": False, "error": None,
                "evidence": {"endpoints": [], "checks": [], "data_access": "NOT_TESTED"},
            }
            logger.info("SKIP %s: duplicate ingestion URL of %s", source["source_id"], seen[key])
        else:
            seen[key] = source["source_id"]
            # Invalidate an earlier success before retrying, including on interruption.
            pending = {
                "source_id": source["source_id"], "display_name": source["display_name"],
                "source_url": source["source_url"], "checked_at_utc": None,
                "attempt_started_at_utc": datetime.now(timezone.utc).isoformat(),
                "connection_status": "NOT_TESTED", "test_status": "IN_PROGRESS",
                "profile": dict(rules.get(source["source_id"], {})),
                "enabled_for_ingestion": False,
            }
            save_result(pending, input_path, input_hash)
            result = assess(source, rules.get(source["source_id"], {}), args)
        result["attempt_started_at_utc"] = started.isoformat() if result.get("duplicate_of") else pending["attempt_started_at_utc"]
        save_result(result, input_path, input_hash)
        results.append(result)
        logger.info("%s: connection=%s checks=%s", source["source_id"], result["connection_status"], result["test_status"])
    GENERATED.mkdir(parents=True, exist_ok=True)
    summary = GENERATED / "source_assessment.csv"
    with summary.open("w", encoding="utf-8", newline="") as handle:
        columns = ["source_id", "source_url", "checked_at_utc", "connection_status", "test_status", "response_format", "http_status", "tls", "duplicate_of", "error"]
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for result in results:
            initial = next((e for e in result["evidence"]["endpoints"] if e["name"] == "initial_resource"), {})
            writer.writerow({**{k: result.get(k) for k in columns if k not in {"response_format", "http_status", "tls"}}, "response_format": result["profile"].get("response_format", "unknown"), "http_status": initial.get("http_status"), "tls": initial.get("tls", "NOT_TESTED")})
    logger.info("Profiles and assessments: %s", GENERATED / "sources")
    logger.info("Assessment: %s", summary)
    return 0 if all(r["test_status"] in {"PASS", "SKIPPED_DUPLICATE"} for r in results) else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        logger.error("Tester could not run: %s", error)
        raise SystemExit(1)

# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Extract public candidate URLs from preserved source-reference files."""

import argparse
import csv
import hashlib
import io
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree

import openpyxl

from src.common.logging import get_logger
from src.common.source_urls import reference_url
from src.common.source_config import ROOT, REFERENCES, GENERATED, source_folder

RAW = REFERENCES
URL_PATTERN = re.compile(r"https?://[^\s<>\"\]\u201c\u201d]+", re.I)
LEGACY_GROUP_NAMES = {
    "G1": "KwaZulu-Natal, South Africa",
    "G2": "Gauteng, South Africa",
    "G3": "Nigeria (Abuja/FCT, Edo, Benue and Delta)",
    "G4": "Kenya",
}
logger = get_logger(__name__)


def endpoint_source_id(url):
    """Name an unnamed source after its host, endpoint and query, never a hash."""
    parts = urlsplit(url)
    description = "_".join(filter(None, (
        parts.hostname, unquote(parts.path), unquote(parts.query), unquote(parts.fragment),
    )))
    name = re.sub(r"[^a-z0-9]+", "_", description.lower()).strip("_")
    return name[:160].rstrip("_")


def assign_source_ids(urls, identities):
    """Keep readable registered names; resolve slug collisions with numbered suffixes."""
    assigned, used = {}, set()
    # Reserve explicit names first so generated names cannot displace them.
    ordered = sorted(urls, key=lambda url: (url not in identities, url))
    for url in ordered:
        registered = identities.get(url, ("", ""))[0]
        base = registered if registered and not registered.startswith("candidate_") else endpoint_source_id(url)
        source_id, number = base, 2
        while source_id in used:
            source_id = f"{base}_{number}"
            number += 1
        used.add(source_id)
        assigned[url] = source_id
    return assigned


def readable_source_name(value):
    """Expand legacy workstream labels in names, never URLs or evidence locators.

    These labels describe the original project scope, not verified data coverage.
    """
    return re.sub(
        r"\bG[1-4]\b",
        lambda match: LEGACY_GROUP_NAMES[match.group().upper()],
        str(value),
        flags=re.I,
    )


def urls_in(value):
    for match in URL_PATTERN.finditer(str(value or "")):
        url = reference_url(match.group())
        if url:
            yield url


def source_values(path):
    """Yield a value and its file locator; never execute document instructions."""
    suffix = path.suffix.lower()
    if suffix in {".csv", ".tsv"}:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            sample = handle.read(8192)
            handle.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
            except csv.Error:
                dialect = csv.excel_tab if suffix == ".tsv" else csv.excel
            for number, row in enumerate(csv.reader(handle, dialect), 1):
                for column, value in enumerate(row, 1):
                    yield value, f"row={number};column={column}"
    elif suffix == ".xlsx":
        book = openpyxl.load_workbook(path, read_only=False, data_only=False, keep_links=False)
        try:
            for sheet in book:
                for row in sheet:
                    for cell in row:
                        if cell.value is not None:
                            yield cell.value, f"sheet={sheet.title};cell={cell.coordinate}"
                        if cell.hyperlink and cell.hyperlink.target:
                            yield cell.hyperlink.target, f"sheet={sheet.title};cell={cell.coordinate};hyperlink"
        finally:
            book.close()
    elif suffix == ".docx":
        with zipfile.ZipFile(path) as archive:
            for member in ("word/document.xml", "word/_rels/document.xml.rels"):
                if member not in archive.namelist():
                    continue
                root = ElementTree.fromstring(archive.read(member))
                for node in root.iter():
                    if node.text:
                        yield node.text, member
                    if node.get("Target"):
                        yield node.get("Target"), member
    elif suffix in {".txt", ".md"}:
        for number, line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
            yield line, f"line={number}"
    else:
        raise ValueError(f"Unsupported reference format: {suffix}")


def existing_identities(files):
    """Keep established source IDs when a reference CSV names its primary URL."""
    identities = {}
    ordered = sorted(files, key=lambda path: (path.name != "source_registry.csv", str(path)))
    for path in ordered:
        if path.suffix.lower() != ".csv":
            continue
        with path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                url = reference_url(row.get("source_url") or row.get("url") or "")
                source_id = row.get("source_id", "").strip()
                if url and source_id:
                    identities.setdefault(url, (source_id, row.get("display_name") or source_id))
    return identities


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument('--url', help='Prepare only this public URL; do not scan reference files')
    inputs.add_argument('--file', type=Path, help='Prepare only this reference file')
    parser.add_argument('--source-id', help='Explicit source ID for --url; otherwise use a readable endpoint name')
    args = parser.parse_args(argv)
    if args.source_id and not args.url:
        parser.error('--source-id requires --url')
    if args.url and list(urls_in(args.url)) != [args.url]:
        parser.error('--url must be one complete public URL without embedded credentials')
    if args.source_id:
        source_folder(args.source_id)
    generated_at = datetime.now(timezone.utc)
    timestamp = generated_at.strftime("%Y%m%d_%H%M%S")
    output = GENERATED / f"urls_{timestamp}.csv"
    report_path = GENERATED / f"preparation_report_{timestamp}.json"
    if output.exists() or report_path.exists():
        raise ValueError("Preparation already ran during this UTC second; retry in the next second.")
    if args.file and not args.file.is_file():
        raise ValueError(f'Reference file is missing: {args.file}')
    if not args.url and not args.file and not RAW.is_dir():
        raise ValueError(f"Raw reference folder is missing: {RAW}")
    files = ([args.file.resolve()] if args.file else [] if args.url else
             sorted(path for path in RAW.rglob("*") if path.is_file() and not path.name.startswith(".")))
    if not files and not args.url:
        raise ValueError("Add reference files to ingestion_points/raw/ first.")
    identities = existing_identities(files)
    found, manifest = {}, []
    accepted_occurrences = 0
    if args.url:
        found[args.url] = {'command_line#url'}
        accepted_occurrences = 1
        if args.source_id:
            identities[args.url] = (args.source_id, urlsplit(args.url).hostname)
    for path in files:
        relative = path.relative_to(ROOT).as_posix() if path.is_relative_to(ROOT) else str(path)
        item = {"file": relative, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "url_occurrences": 0}
        try:
            for value, locator in source_values(path):
                for url in urls_in(value):
                    found.setdefault(url, set()).add(f"{relative}#{locator}")
                    item["url_occurrences"] += 1
                    accepted_occurrences += 1
            item["status"] = "processed"
        except Exception as error:
            item.update(status="failed", error_type=type(error).__name__)
            logger.error("Could not inspect %s (%s)", relative, type(error).__name__)
        manifest.append(item)
    if not found:
        raise ValueError("No candidate URLs extracted; existing output was not replaced.")
    rows = []
    assigned = assign_source_ids(found, identities)
    for url, locations in sorted(found.items()):
        source_id = assigned[url]
        name = identities.get(url, (None, urlsplit(url).hostname))[1]
        rows.append({"source_id": source_id, "display_name": readable_source_name(name), "source_url": url, "evidence_locations": json.dumps(sorted(locations), ensure_ascii=False)})
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=["source_id", "display_name", "source_url", "evidence_locations"], lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    GENERATED.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(".csv.tmp")
    temp.write_text(buffer.getvalue(), encoding="utf-8")
    temp.replace(output)
    report = {"generated_at_utc": generated_at.isoformat(), "url_file": output.name, "files": manifest, "unique_urls": len(rows), "url_identity_policy": "Exact extracted URL strings only; no URL normalization or dataset deduplication.", "accepted_url_occurrences": accepted_occurrences, "exact_repeated_occurrences": accepted_occurrences - len(rows), "failed_files": sum(item["status"] == "failed" for item in manifest), "scope": "Candidate URL extraction only. No source accessed, prices ingested, or source availability inferred."}
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    logger.info("Prepared %s candidate URLs from %s reference files: %s", len(rows), len(files), output)
    logger.info("Preparation report: %s", report_path)
    return 1 if report["failed_files"] else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        logger.error("Preparation failed: %s", error)
        raise SystemExit(1)

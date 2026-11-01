# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Bounded discovery on public pages; preserve raw tables and document text.

No guessed endpoints or price mappings. Every output points to a saved response.
"""

import csv
import hashlib
import json
import re
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit
from zipfile import BadZipFile

from src.common.source_config import DATA
from src.common.source_testing import Client, Evidence, Page, ProbeError, tls_bundle
from src.common.source_urls import clean_url
from src.common.record_output import write_source_records
from src.common.tabular_files import tabular_sheets


class NeedsAdapter(ProbeError):
    """The response is accessible, but this run cannot extract its records."""


def json_record_lists(value, path='root', depth=0):
    """Expose actual object lists without guessing that they contain prices."""
    if depth > 4:
        return
    if isinstance(value, list) and value and all(isinstance(row, dict) for row in value):
        yield path, value
    elif isinstance(value, dict):
        for key, child in value.items():
            if isinstance(child, (dict, list)):
                yield from json_record_lists(child, f'{path}.{key}', depth+1)


def linked_resources(body, current):
    """Only follow actual anchors, keeping navigation on the publisher's host."""
    found = {}
    for link in Page(body.decode('utf-8-sig', errors='replace')).links:
        url = clean_url(urljoin(current, link['href']))
        if not url or urlsplit(url).scheme != 'https':
            continue
        hint = ' '.join(link['text']).lower()
        path = urlsplit(url).path.lower()
        download = (path.endswith(('.csv', '.xlsx', '.zip', '.pdf')) or '/download/' in path)
        navigation = (urlsplit(url).hostname == urlsplit(current).hostname
                      and re.search(r'data|price|market|catalog|material|statistic|commodit|food|agric', hint + ' ' + path))
        if download or navigation:
            found.setdefault(url, {'url': url, 'download': bool(download)})
    return list(found.values())


def collect_web_source(source, *, max_seconds):
    """Try public tables, linked files and PDF text within explicit budgets."""
    from src.source_tester import inspect_response

    sid = source['source_id']
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    raw, output = DATA/'raw'/sid/stamp, DATA/'collected'/sid/stamp
    raw.mkdir(parents=True, exist_ok=False)
    output.mkdir(parents=True, exist_ok=False)
    report = dict(source_id=sid, run_id=stamp, source_url=source['source_url'],
                  started_at_utc=datetime.now(timezone.utc).isoformat(), status='IN_PROGRESS',
                  completeness='UNKNOWN', records_saved=0, exports=[], responses=[], failures=[],
                  scope='Bounded actual page links, raw HTML/workbook tables and PDF text; business meaning is unverified.')
    evidence = Evidence(source)
    settings = source.get('profile', {})
    # Limits prevent a homepage turning into an unlimited crawl. Unvisited URLs
    # are reported, rather than silently called complete or unrelated.
    page_limit = int(settings.get('page_limit', 6))
    file_limit = int(settings.get('download_limit', 10))
    if not 1 <= page_limit <= 50 or not 1 <= file_limit <= 100:
        raise ValueError('page_limit must be 1–50; download_limit must be 1–100.')
    queue = deque([(source['source_url'], 0, False)])
    seen, deferred, tables_seen = set(), [], set()
    pages = files = 0

    def export(label, rows, response, kind, truncated=False):
        if not rows:
            return
        index = len(report['exports'])
        csv_path = output/f'table_{index:03d}_{sid}_{stamp}.csv'
        jsonl = csv_path.with_suffix('.jsonl')
        write_source_records(rows, jsonl, csv_path)
        report['exports'].append(dict(label=label, csv_file=str(csv_path), records_file=str(jsonl),
                                      rows=len(rows), response_file=str(response),
                                      extraction_kind=kind, row_limit_reached=truncated))
        report['records_saved'] += len(rows)

    with tls_bundle(settings) as bundle:
        client = Client(evidence, verify=bundle, timeout=30, max_seconds=max_seconds, max_bytes=32_000_000)
        try:
            while queue and time.monotonic() < client.deadline:
                url, depth, download = queue.popleft()
                if url in seen:
                    continue
                if (download and files >= file_limit) or (not download and pages >= page_limit):
                    deferred.append(url)
                    continue
                seen.add(url)
                if download:
                    files += 1
                else:
                    pages += 1
                try:
                    body, final = client.get(f'resource_{len(seen):03d}', url)
                    seen.add(final)
                    endpoint = evidence.report['endpoints'][-1]
                    kind = inspect_response(body, endpoint.get('content_type', ''), endpoint['complete'])['response_format']
                    response = raw/f'response_{len(report["responses"]):03d}.{kind if kind != "unknown" else "bin"}'
                    response.write_bytes(body)
                    report['responses'].append(dict(file=str(response), url=final, bytes=len(body),
                                                   sha256=hashlib.sha256(body).hexdigest(),
                                                   retrieved_at_utc=datetime.now(timezone.utc).isoformat()))
                    if kind == 'html':
                        page = Page(body.decode('utf-8-sig', errors='replace'))
                        for index, table in enumerate(page.tables):
                            values = [r.cells for r in table if any(r.cells)]
                            # Raw table grids are explicitly unclassified; do not
                            # mislabel page navigation or infer price/date fields.
                            if len(values) < 2 or max(map(len, values)) < 2 or not any(re.search(r'\d', c) for r in values for c in r):
                                continue
                            signature = hashlib.sha256(json.dumps([final, values]).encode()).hexdigest()
                            if signature in tables_seen:
                                continue
                            tables_seen.add(signature)
                            rows = [dict(row_number=n, **{f'column_{i}': c for i, c in enumerate(r, 1)})
                                    for n, r in enumerate(values[:10000], 1)]
                            export(f'HTML table {index}', rows, response, 'unclassified_html_table', len(values)>10000)
                        links = linked_resources(body, final)
                        for item in links:
                            if item['url'] in seen:
                                continue
                            if not item['download'] and depth >= 2:
                                deferred.append(item['url'])
                            else:
                                queue.append((item['url'], depth+1, item['download']))
                    elif kind in {'csv', 'xlsx', 'zip'}:
                        for label, rows, truncated in tabular_sheets(body, kind, deadline=client.deadline, byte_limit=32_000_000):
                            export(label, rows, response, 'workbook_grid' if kind != 'csv' else 'source_csv', truncated)
                    elif kind == 'pdf':
                        text = response.with_suffix('.txt')
                        result = subprocess.run(['pdftotext', '-layout', str(response), str(text)],
                                                capture_output=True, timeout=max(1, min(30, client.deadline-time.monotonic())))
                        if result.returncode:
                            raise ProbeError('PDF retained; text extraction failed or the document needs OCR.')
                        rows = []
                        for page_number, page_text in enumerate(text.read_text(errors='replace').split('\f'), 1):
                            for line_number, line in enumerate(page_text.splitlines(), 1):
                                if line.strip():
                                    rows.append(dict(page_number=page_number, line_number=line_number, text=line))
                        export('PDF text, not parsed price observations', rows[:100000], response, 'document_text', len(rows)>100000)
                        if not rows:
                            raise ProbeError('PDF retained; no text found. OCR may be needed.')
                    elif kind == 'json':
                        payload = json.loads(body)
                        if isinstance(payload, dict) and (payload.get('success') is False or payload.get('error')):
                            raise ProbeError('API returned an application error; response retained.')
                        for label, rows in json_record_lists(payload):
                            export(label, rows[:10000], response, 'unclassified_api_records', len(rows)>10000)
                        report['api_pagination'] = 'UNKNOWN: generic list extraction; use reviewed API rules for complete traversal.'
                    else:
                        raise NeedsAdapter('Response retained; this format needs a reviewed parser or API record path.')
                except (ProbeError, ValueError, OSError, csv.Error, BadZipFile, subprocess.TimeoutExpired) as error:
                    report['failures'].append(dict(url=url, error=str(error)))
        finally:
            client.close()
            report['unvisited_urls'] = list(dict.fromkeys(deferred + [q[0] for q in queue]))
            report['request_evidence'] = evidence.report
            report['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
            limited = report['unvisited_urls'] or report['failures'] or any(e['row_limit_reached'] for e in report['exports'])
            report['status'] = ('PARTIAL' if limited else 'FETCHED_RESPONSE') if report['records_saved'] else 'NEEDS_ADAPTER'
            (raw/f'collection_report_{sid}_{stamp}.json').write_text(json.dumps(report, indent=2)+'\n')
    if not report['records_saved']:
        raise NeedsAdapter('No records extracted; see saved responses, failures and unvisited links.')
    return report['status'] == 'FETCHED_RESPONSE'

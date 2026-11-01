# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Build data/exports/latest from the latest available data for each source.

Failed new downloads never erase an earlier useful collection. The manifest
identifies older data reused after a failure and keeps all source evidence paths.
"""

import argparse
import csv
import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

from src.common.source_config import DATA, GENERATED
from src.common.record_output import export_source_jsonl_csv
from src.common.logging import get_logger

logger = get_logger(__name__)


def csv_rows(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return max(0, sum(1 for _ in csv.reader(stream))-1)


def build_export():
    DATA.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    staging = DATA/'exports'/f'.building_{stamp}'
    (staging/'csv').mkdir(parents=True, exist_ok=False)
    (staging/'metadata').mkdir()
    manifest = dict(status='IN_PROGRESS', assembled_at_utc=datetime.now(timezone.utc).isoformat(), files=[],
                    scope='Latest available raw tables per source. Includes workbook grids, unclassified HTML tables and document text. These are not all unique price observations.')
    summaries = []
    try:
        for source_dir in sorted((DATA/'raw').glob('*')):
            if not source_dir.is_dir():
                continue
            reports = []
            for path in sorted(source_dir.glob('*/collection_report*.json'), reverse=True):
                value = json.loads(path.read_text())
                reports.append((path, value))
            if not reports:
                continue
            latest = reports[0][1]
            selected = next(((p, d) for p, d in reports if d.get('records_saved', 0) > 0
                             and (d.get('exports') or d.get('csv_file'))), None)
            if selected is None:
                summaries.append(dict(source_id=source_dir.name, file='', rows=0, data_kind='',
                                      latest_attempt=latest.get('status'), reused_earlier_data=False,
                                      issue=latest.get('error', 'No extracted records available.')))
                continue
            report_path, report = selected
            sid = report['source_id']
            reused = report is not latest
            metadata = staging/'metadata'/f'{sid}.json'
            shutil.copy2(report_path, metadata)
            # API checkpoints list every completed run of the selected series.
            # Preserve raw repeats; reporting keys and correction policy come later.
            series = report.get('series_runs')
            if series:
                inputs = list(dict.fromkeys(Path(r['records_file']) for r in series if r['rows']))
                target = staging/'csv'/f'{sid}.csv'
                _, count = export_source_jsonl_csv(inputs, target)
                if count != sum(r['rows'] for r in series):
                    raise ValueError(f'API series count mismatch: {sid}')
                exports = [dict(target=target, rows=count, inputs=list(map(str, inputs)), kind='api_records', label='API collection series')]
            else:
                items = report.get('exports') or [dict(csv_file=report['csv_file'], rows=report['records_saved'], label='Source records')]
                exports = []
                for index, item in enumerate(items):
                    original = Path(item['csv_file'])
                    count = csv_rows(original)
                    if count != item['rows']:
                        raise ValueError(f'CSV count mismatch: {original}')
                    label = re.sub(r'[^a-z0-9]+', '_', item.get('label', 'records').lower()).strip('_')[:65]
                    target = staging/'csv'/f'{sid}_{index:02d}_{label}.csv'
                    shutil.copy2(original, target)
                    exports.append(dict(target=target, rows=count, inputs=[str(original)],
                                        kind=item.get('extraction_kind', 'workbook_grid' if report.get('excel_policy') else 'source_records'), label=item.get('label', 'Source records')))
            for item in exports:
                entry = dict(source_id=sid, source_url=report['source_url'], file=str(item['target'].relative_to(staging)),
                             rows=item['rows'], data_kind=item['kind'], label=item['label'],
                             inputs=item['inputs'], source_report=str(metadata.relative_to(staging)),
                             original_source_report=str(report_path),
                             retrieved_at_utc=report.get('retrieved_at_utc', report.get('started_at_utc')),
                             sha256=hashlib.sha256(item['target'].read_bytes()).hexdigest(),
                             collection_status=report['status'], latest_attempt=latest.get('status'),
                             reused_earlier_data=reused, scope_completion=report.get('scope_completion', report.get('completeness', 'UNKNOWN')),
                             observation_dates=report.get('observation_dates', []))
                manifest['files'].append(entry)
                summaries.append(dict(source_id=sid, file=entry['file'], rows=entry['rows'], data_kind=entry['data_kind'],
                                      latest_attempt=entry['latest_attempt'], reused_earlier_data=reused,
                                      issue='Earlier data retained after a later unsuccessful attempt.' if reused else ''))
        manifest.update(status='FINISHED', csv_files=len(manifest['files']),
                        raw_rows=sum(f['rows'] for f in manifest['files']))
        # Include every candidate in the handover summary, even when it never
        # reached the collector. This keeps access failures visible downstream.
        inventories = sorted(GENERATED.glob('urls_*.csv'))
        candidates = {}
        if inventories:
            with inventories[-1].open(encoding='utf-8-sig', newline='') as stream:
                candidates = {r['source_id']: r for r in csv.DictReader(stream)}
            manifest['inventory_file'] = str(inventories[-1])
        present = {r['source_id'] for r in summaries}
        for sid, source in candidates.items():
            if sid in present:
                continue
            assessment_path = GENERATED/'sources'/sid/'assessment.json'
            assessment = json.loads(assessment_path.read_text()) if assessment_path.exists() else {}
            summaries.append(dict(source_id=sid, file='', rows=0, data_kind='',
                                  latest_attempt='ACCESS_FAILED' if assessment.get('connection_status') == 'FAIL' else 'NOT_COLLECTED',
                                  reused_earlier_data=False,
                                  issue=assessment.get('error') or 'No collector output yet; review source assessment.'))
        urls = {f['source_id']: f['source_url'] for f in manifest['files']}
        for row in summaries:
            source = candidates.get(row['source_id'], {})
            row['source_name'] = source.get('display_name', row['source_id'])
            row['source_url'] = source.get('source_url', urls.get(row['source_id'], ''))
        (staging/'export_report.json').write_text(json.dumps(manifest, indent=2)+'\n')
        with (staging/'source_summary.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=['source_id', 'source_name', 'source_url', 'file', 'rows', 'data_kind', 'latest_attempt', 'reused_earlier_data', 'issue'])
            writer.writeheader()
            writer.writerows(summaries)
        (staging/'README.txt').write_text(
            'START HERE\n\n'
            'csv/ contains the files for the next processing stage.\n'
            'source_summary.csv lists available and unsuccessful sources, including older data reused after a failure.\n'
            'export_report.json records original inputs, checksums, extraction type and coverage.\n'
            'metadata/ contains the collection reports, so the handover keeps its context when copied.\n'
            'Workbook grids, HTML tables and PDF text need interpretation before becoming reporting observations.\n'
            'Source dates remain as supplied; collection time is not an observation date.\n')
        # Copy the current source review alongside the data, without changing it.
        for name in ['batch_collection.json']:
            if (GENERATED/name).exists():
                shutil.copy2(GENERATED/name, staging/name)
        latest_folder = DATA/'exports/latest'
        if latest_folder.exists():
            archive = DATA/'archive/exports'/stamp
            archive.parent.mkdir(parents=True, exist_ok=True)
            latest_folder.rename(archive)
        staging.rename(latest_folder)
        logger.info('Ready: %s CSV files, %s raw rows: %s', manifest['csv_files'], manifest['raw_rows'], latest_folder)
        return manifest
    except BaseException:
        # Keep the last published handover intact if a new export cannot finish.
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    build_export()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

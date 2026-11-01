"""Offline workflow checks: synthetic responses, temporary files, no cloud writes."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class PipelineWorkflowTests(unittest.TestCase):
    def run_workflow(self, mode):
        with tempfile.TemporaryDirectory() as directory:
            environment = dict(os.environ, OTA_WORKFLOW_MODE=mode,
                               OTA_STORAGE_DIR=str(Path(directory) / 'storage'),
                               OTA_REFERENCE_DIR=str(Path(directory) / 'references'))
            result = subprocess.run(
                [sys.executable, '-c',
                 "import runpy; runpy.run_path('tests/test_pipeline_workflows.py', run_name='workflow_child')"],
                cwd=Path(__file__).resolve().parents[1], env=environment,
                capture_output=True, text=True, timeout=45,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn('WORKFLOW PASS', result.stdout)

    def test_single_url_through_export_preparation_and_simulated_load(self):
        self.run_workflow('single')

    def test_direct_url_through_simulated_load(self):
        self.run_workflow('direct')

    def test_explicit_reference_file_through_simulated_load(self):
        self.run_workflow('file')

    def test_batch_formats_pagination_failures_and_simulated_load(self):
        self.run_workflow('batch')


def workflow_child():
    import csv
    import io
    import json
    from contextlib import ExitStack
    from unittest.mock import patch
    from urllib.parse import urlsplit

    import requests
    from openpyxl import Workbook
    from src import source_tester, collector, batch_pipeline, run_pipeline, prepare_sources
    from src.common.source_config import DATA, GENERATED, REFERENCES, load_candidates
    from src.export_batch import build_export
    from src.prepare_bigquery import build_package
    from src.load_bigquery import validate_package, run_cloud
    from tests.test_load_bigquery import FakeClient

    mode = os.environ['OTA_WORKFLOW_MODE']
    REFERENCES.mkdir(parents=True)
    urls = {'fixture_csv': 'https://csv.example.test/prices.csv'}
    if mode == 'batch':
        urls.update({
            'fixture_excel': 'https://excel.example.test/prices.xlsx',
            'fixture_html': 'https://html.example.test/prices',
            'fixture_api': 'https://api.example.test/prices',
            'fixture_denied': 'https://denied.example.test/prices',
            'fixture_tls': 'https://tls.example.test/prices',
        })
    with (REFERENCES / 'sources.csv').open('w', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(['source_id', 'display_name', 'source_url'])
        for sid, url in urls.items():
            writer.writerow([sid, 'Synthetic workflow fixture', url])
        writer.writerow(['fixture_csv', 'Repeated URL fixture', urls['fixture_csv']])

    workbook = Workbook()
    if mode == 'file':
        (REFERENCES / 'unselected.csv').write_text(
            'source_url\nhttps://unselected.example.test/prices\n')
    if mode == 'direct':
        # Direct URL input must work without any reference folder.
        (REFERENCES / 'sources.csv').unlink()
        REFERENCES.rmdir()
    workbook.active.append(['commodity', 'price', 'date'])
    workbook.active.append(['Test crop', '12.50', '2024-01-02'])
    excel = io.BytesIO()
    workbook.save(excel)
    calls = []
    api_pages = []
    rules = {'fixture_api': {
        'records_path': 'data',
        'collection': {'min_interval_seconds': 0, 'pagination': {
            'mode': 'page', 'parameter': 'page', 'page_size': 2,
            'size_parameter': 'limit', 'total_path': 'total',
        }},
    }}

    def response(method, url, **kwargs):
        # Exercise the real Client; replace only its network transport.
        assert kwargs['verify'] is not False, 'TLS verification was disabled'
        assert url in urls.values(), 'Unexpected endpoint: ' + url
        calls.append(url)
        host = urlsplit(url).hostname
        status, content_type = 200, 'text/csv'
        if host == 'tls.example.test':
            raise requests.exceptions.SSLError('Synthetic TLS failure')
        if host == 'denied.example.test':
            status, body = 401, b'Authentication required'
        elif host == 'csv.example.test':
            if calls.count(url) == 1:
                status, body = 429, b'Rate limited'
            else:
                body = b'commodity,price,date\nTest crop A,10.00,2024-01-02\nTest crop B,20.00,2024-01-02\n'
        elif host == 'excel.example.test':
            content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            body = excel.getvalue()
        elif host == 'html.example.test':
            content_type = 'text/html'
            body = b'<html><table><tr><th>Commodity</th><th>Price</th></tr><tr><td>Test crop</td><td>15</td></tr></table></html>'
        elif host == 'api.example.test':
            content_type = 'application/json'
            page = int((kwargs.get('params') or {}).get('page', 1))
            if kwargs.get('params') and 'page' in kwargs['params']:
                api_pages.append(page)
            rows = [{'commodity': 'Test crop ' + str(i), 'price': i,
                     'date': '2024-01-02'} for i in ([1, 2] if page == 1 else [3])]
            body = json.dumps({'data': rows, 'total': 3}).encode()
        else:
            raise AssertionError('Unconfigured fixture endpoint')
        result = requests.Response()
        result.status_code = status
        result.url = url
        result.headers = {'Content-Type': content_type, 'Content-Length': str(len(body))}
        if status == 429:
            result.headers['Retry-After'] = '0'
        result._content = body
        result._content_consumed = True
        return result

    with ExitStack() as stack:
        stack.enter_context(patch('requests.sessions.Session.request', side_effect=response))
        for module in [source_tester, collector, batch_pipeline]:
            stack.enter_context(patch.object(module, 'load_testing_rules', return_value=rules))
        if mode != 'batch':
            arguments = (['--url', urls['fixture_csv'], '--source-id', 'fixture_csv'] if mode == 'direct'
                         else ['--file', str(REFERENCES / 'sources.csv')] if mode == 'file' else [])
            assert prepare_sources.main(arguments) == 0
            candidates, _ = load_candidates()
            assert len(candidates) == 1, 'Exact repeated URL was not combined'
            with patch.object(sys, 'argv', ['ota-test', '--source-id', 'fixture_csv']):
                # Generic CSV access is verified, but price semantics remain
                # unverified: the tester must report PARTIAL, not approval.
                assert source_tester.main() == 1
            assessment = json.loads((GENERATED / 'sources/fixture_csv/assessment.json').read_text())
            assert assessment['connection_status'] == 'PASS'
            assert assessment['test_status'] == 'PARTIAL'
            with patch.object(sys, 'argv', ['ota-collect', '--source-id', 'fixture_csv']):
                assert collector.main() == 0
            export = build_export()
            assert export['raw_rows'] == 2
        else:
            with patch.object(sys, 'argv', ['ota-batch', '--workers', '2', '--timeout', '2', '--max-seconds', '10']):
                assert run_pipeline.main() == 1, 'Access failures must make the batch partial'
            candidates, _ = load_candidates()
            assert len(candidates) == 6
            batch = json.loads((GENERATED / 'batch_collection.json').read_text())
            states = {row['source_id']: row['status'] for row in batch['sources']}
            for sid in ['fixture_csv', 'fixture_excel', 'fixture_html', 'fixture_api']:
                assert states[sid] == 'COLLECTED', (sid, states)
            for sid in ['fixture_denied', 'fixture_tls']:
                assert states[sid] == 'ACCESS_FAILED', (sid, states)
            assert api_pages == [1, 2], api_pages
            job = json.loads((DATA / 'job_report.json').read_text())
            assert job['status'] == 'PARTIAL'
            export = json.loads((DATA / 'exports/latest/export_report.json').read_text())
            api_files = [entry for entry in export['files'] if entry['source_id'] == 'fixture_api']
            assert sum(entry['rows'] for entry in api_files) == 3
        assert calls.count(urls['fixture_csv']) >= 3, '429 retry did not occur'

    # Prepare and reconcile the exported data using the production loader logic,
    # but a local FakeClient in place of Google BigQuery.
    package = DATA / 'bigquery/latest'
    prepared = build_package(DATA / 'exports/latest', package, 'sample-project', 'landing', 'europe-west3')
    assert prepared['raw_rows'] == export['raw_rows']
    manifest, tables, checksum = validate_package(package)
    client = FakeClient()
    state = DATA / 'bigquery/state'
    checked = run_cloud(manifest, tables, checksum, client, 'check', state)
    assert checked['status'] == 'FINISHED'
    assert client.loads == 0 and client.creates == 0
    loaded = run_cloud(manifest, tables, checksum, client, 'execute', state)
    assert loaded['status'] == 'FINISHED', loaded
    assert sum(table.num_rows for table in client.tables.values()) == export['raw_rows']
    load_count = client.loads
    retried = run_cloud(manifest, tables, checksum, client, 'execute', state)
    assert retried['status'] == 'FINISHED' and client.loads == load_count
    print('WORKFLOW PASS:', mode, export['csv_files'], 'CSV files,', export['raw_rows'], 'synthetic rows')


if __name__ == 'workflow_child':
    workflow_child()

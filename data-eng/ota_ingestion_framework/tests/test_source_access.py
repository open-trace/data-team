"""Offline checks for published downloads and access failures."""

import io
import json
import tempfile
import time
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

from openpyxl import Workbook

from src.collector import collect_file_source
from src.common.source_access import access_failure, download_links
from src.common.source_testing import ProbeError


class SourceAccessTests(unittest.TestCase):
    def test_links_keep_published_cross_host_and_relative_targets(self):
        body = b'''<a href="/prices.xlsx">Prices</a><a href="/prices.xlsx">Again</a>
        <a href="https://files.example.org/data.csv">CSV</a>
        <a href="http://example.org/insecure.zip">Insecure</a>
        <a href="/private.xlsx?token=secret">Private</a><a href="/report.pdf">PDF</a>'''
        links = download_links(body, 'https://example.org/catalogue')
        self.assertEqual([x['url'] for x in links],
                         ['https://example.org/prices.xlsx', 'https://files.example.org/data.csv'])

    def test_403_is_not_a_transient_retry(self):
        result = access_failure({'http_status': 403, 'tls': 'PASS'})
        self.assertEqual(result['failure_code'], 'ACCESS_RESTRICTED')
        self.assertFalse(result['retryable'])

    def test_tls_failure_keeps_verification(self):
        result = access_failure({'tls': 'FAIL', 'http_status': None})
        self.assertFalse(result['retryable'])
        self.assertIn('keep verification enabled', result['next_action'])

    def test_interrupted_body_is_not_success(self):
        result = access_failure({'http_status': 200, 'tls': 'PASS', 'error': 'Connection failed.'})
        self.assertEqual(result['failure_code'], 'DOWNLOAD_INTERRUPTED')
        self.assertTrue(result['retryable'])

    def test_rate_limit_requires_cooldown(self):
        self.assertEqual(access_failure({'http_status': 429})['failure_code'], 'COOLDOWN_REQUIRED')

    def collect_fixture(self, *, linked=True, bad_file=False, limit=None):
        workbook = Workbook()
        workbook.active.append(['Commodity', 'Price'])
        workbook.active.append(['Tomato', 50])
        stream = io.BytesIO()
        workbook.save(stream)
        xlsx = stream.getvalue()
        html = b'<a href="/one.xlsx">One</a><a href="/two.xlsx">Two</a>'

        class FixtureClient:
            def __init__(self, evidence, **kwargs):
                self.evidence = evidence
                self.deadline = time.monotonic() + 30

            def get(self, name, url):
                is_page = url.endswith('/page')
                body = html if is_page or bad_file else xlsx
                self.evidence.report['endpoints'].append(
                    {'name': name, 'status': 'PASS', 'complete': True,
                     'http_status': 200, 'tls': 'PASS',
                     'content_type': 'text/html' if is_page or bad_file else 'application/octet-stream'})
                return body, url

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            profile = {'fetch_method': 'linked_downloads'} if linked else {'response_format': 'xlsx'}
            if limit is not None:
                profile['download_limit'] = limit
            source = {'source_id': 'fixture', 'display_name': 'Offline fixture', 'source_url': 'https://example.org/page' if linked else 'https://example.org/one.xlsx',
                      'profile': profile, 'test_status': 'PARTIAL'}
            with patch('src.collector.DATA', root / 'data'), patch('src.collector.Client', FixtureClient), \
                    patch('src.collector.tls_bundle', return_value=nullcontext(True)):
                if bad_file:
                    with self.assertRaises(ProbeError):
                        collect_file_source(source, max_seconds=30)
                else:
                    collect_file_source(source, max_seconds=30)
            report = json.loads(next(root.glob('data/raw/fixture/*/collection_report*.json')).read_text())
            return report, len(list(root.glob('data/collected/fixture/*/*.csv')))

    def test_html_page_exports_real_workbook_tables(self):
        report, count = self.collect_fixture()
        self.assertEqual(count, 2)
        self.assertEqual(report['records_saved'], 4)
        self.assertEqual(report['status'], 'FETCHED_RESPONSE')
        self.assertEqual(len(report['responses']), 2)
        self.assertTrue(all(e['resource_url'].endswith('.xlsx') for e in report['exports']))

    def test_limit_leaves_partial_results(self):
        report, count = self.collect_fixture(limit=1)
        self.assertEqual(count, 1)
        self.assertEqual(report['links_unvisited'], 1)
        self.assertEqual(report['status'], 'PARTIAL')

    def test_html_masquerading_as_xlsx_produces_no_records(self):
        report, count = self.collect_fixture(bad_file=True)
        self.assertEqual(count, 0)
        self.assertEqual(report['status'], 'FAIL')
        self.assertTrue(report['failures'])

    def test_direct_workbook_still_exports(self):
        report, count = self.collect_fixture(linked=False)
        self.assertEqual(count, 1)
        self.assertEqual(report['records_saved'], 2)


if __name__ == '__main__':
    unittest.main()

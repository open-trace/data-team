import csv
import json
import tempfile
import time
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

from src.export_batch import build_export
from src.common.web_collection import linked_resources, collect_web_source, json_record_lists
from src.common.parsing import parse_tshwane_statistics


class HandoverTests(unittest.TestCase):
    def test_generic_json_lists_preserve_different_record_shapes(self):
        result = list(json_record_lists({'data': [{'crop': 'Tomato', 'price': None}], 'other': {'rows': [{'volume': 5}]}}))
        self.assertEqual([path for path, _ in result], ['root.data', 'root.other.rows'])
        self.assertIsNone(result[0][1][0]['price'])

    def test_generic_page_extracts_grid_and_follows_a_csv_link(self):
        html = b'<table><tr><th>Crop</th><th>Price</th></tr><tr><td>Tomato</td><td>50</td></tr></table><a href="/prices.csv">Download</a>'
        class FixtureClient:
            def __init__(self, evidence, **kwargs):
                self.evidence = evidence
                self.deadline = time.monotonic()+30
            def get(self, name, url):
                body, kind = (b'crop,price\nTomato,50\n', 'text/csv') if url.endswith('.csv') else (html, 'text/html')
                self.evidence.report['endpoints'].append(dict(content_type=kind, complete=True))
                return body, url
            def close(self):
                pass
        with tempfile.TemporaryDirectory() as folder:
            source = dict(source_id='fixture', display_name='Fixture only', source_url='https://example.org/', profile={})
            with patch('src.common.web_collection.DATA', Path(folder)), patch('src.common.web_collection.Client', FixtureClient), patch('src.common.web_collection.tls_bundle', return_value=nullcontext(True)):
                self.assertTrue(collect_web_source(source, max_seconds=30))
            report = json.loads(next(Path(folder).glob('raw/fixture/*/collection_report*.json')).read_text())
            self.assertEqual(report['records_saved'], 3)
            self.assertEqual({e['extraction_kind'] for e in report['exports']}, {'unclassified_html_table', 'source_csv'})
            self.assertTrue(all(Path(e['response_file']).exists() for e in report['exports']))

    def test_real_links_include_pdf_and_catalogue_download_routes(self):
        body = b'<a href="/catalog/162/download/1427">Download</a><a href="/report.pdf">Report</a><a href="/about">About us</a>'
        links = linked_resources(body, 'https://example.org/')
        self.assertEqual([x['url'] for x in links], ['https://example.org/catalog/162/download/1427', 'https://example.org/report.pdf'])

    def test_negative_source_quantity_is_retained_and_flagged(self):
        fields = {'Product': 'TOMATO', 'Grade': '1', 'Container': 'BOX', 'Count': '', 'Mass': '10',
                  'Lowest Price': '1', 'Average Price': '2', 'Highest Price': '3', 'Quantity Sold': '-5', 'Value Of Sales': '-10'}
        body = ('<p>Data as at 23 October 2026</p><table>' + ''.join(f'<tr><td>{k}</td><td>{v}</td></tr>' for k,v in fields.items())+'</table>').encode()
        package = dict(grade_source='1', container_source='BOX', count_source='', mass_source='10', package_id_source='1')
        record = parse_tshwane_statistics(body, {'product_name_source': 'TOMATO'}, package,
                                         source_id='fixture', retrieved_at='2026-10-24T00:00:00+00:00')
        self.assertEqual(record['quantity_sold'], '-5')
        self.assertTrue(any('quantitysold' in issue for issue in record['quality_issues']))

    def test_failed_retry_does_not_erase_previous_export(self):
        with tempfile.TemporaryDirectory() as folder:
            data = Path(folder)/'data'
            old = data/'raw/fixture/001';old.mkdir(parents=True)
            new = data/'raw/fixture/002';new.mkdir()
            original = data/'collected/fixture/001/data.csv';original.parent.mkdir(parents=True)
            original.write_text('item,value\nTomato,50\n')
            report = dict(source_id='fixture', source_url='https://example.org/data.csv',
                          status='FETCHED_RESPONSE', records_saved=1, csv_file=str(original))
            (old/'collection_report_fixture_001.json').write_text(json.dumps(report))
            (new/'collection_report_fixture_002.json').write_text(json.dumps(dict(status='FAIL', records_saved=0, error='Timeout')))
            with patch('src.export_batch.DATA',data), patch('src.export_batch.GENERATED',Path(folder)/'state'):
                result = build_export()
                self.assertEqual(result['raw_rows'], 1)
                self.assertTrue(result['files'][0]['reused_earlier_data'])
                self.assertEqual(result['files'][0]['latest_attempt'], 'FAIL')
                latest = data/'exports/latest/export_report.json'
                previous = latest.read_bytes()
                original.write_text('item,value\nTomato,50\nExtra,60\n')
                with self.assertRaises(ValueError):
                    build_export()
                self.assertEqual(latest.read_bytes(), previous)


if __name__ == '__main__':
    unittest.main()

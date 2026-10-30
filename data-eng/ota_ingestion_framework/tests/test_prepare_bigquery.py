import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from src.prepare_bigquery import build_package, field_names, group_name


class BigQueryPreparationTests(unittest.TestCase):
    def test_tshwane_has_its_own_logical_landing_table(self):
        self.assertEqual(group_name({'source_id':'za_tshwane_market_fresh','data_kind':'source_records'},'MARKET_PRICE_ROWS'),
                         'farmlover_tshwane_prices')

    def fixture(self, root):
        handover=root/'handover';(handover/'csv').mkdir(parents=True)
        (handover/'metadata').mkdir()
        files=[]
        for index,name in enumerate(['coffee','corn']):
            columns=['name','price','unit','currency_unit','updated','comment']
            values=[name,'001.20','lb','USX','1792789200','Quoted "value"\nUnicode: é; empty next']
            if index==0:
                columns+=['empty','optional_field'];values+=['','source value']
            path=handover/f'csv/{name}.csv'
            with path.open('w',newline='') as f:csv.writer(f).writerows([columns,values])
            report=f'metadata/{name}.json'
            (handover/report).write_text(json.dumps({'run_id':'source_run'}))
            files.append(dict(file=f'csv/{name}.csv',sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                              rows=1,source_id='api_api_ninjas_com_v1_commodityprice_name_'+name,
                              source_url='https://example.com/?name='+name,data_kind='api_records',
                              label='API records',source_report=report,retrieved_at_utc='2026-10-24T12:00:00+00:00'))
        (handover/'export_report.json').write_text(json.dumps(dict(status='FINISHED',csv_files=2,raw_rows=2,files=files)))
        return handover

    def test_compatible_quotes_preserve_values_and_lineage(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);handover=self.fixture(root);output=root/'prepared'
            result=build_package(handover,output,'sample-project','landing','europe-west3')
            self.assertEqual(result['proposed_tables'],1)
            self.assertEqual(result['raw_rows'],2)
            table=result['tables'][0]
            rows=[json.loads(line) for line in (output/table['data_file']).read_text().splitlines()]
            self.assertEqual(rows[0]['price'],'001.20')
            self.assertEqual(rows[0]['empty'],'')
            self.assertIsNone(rows[1]['optional_field'])
            self.assertIn('\nUnicode: é',rows[0]['comment'])
            self.assertEqual(rows[0]['_fl_collection_run_id'],'source_run')
            self.assertEqual(rows[0]['_fl_input_row_number'],1)
            self.assertEqual(rows[0]['_fl_source_id'].rsplit('_',1)[-1],'coffee')
            self.assertEqual(table['load_job_config']['write_disposition'],'WRITE_EMPTY')
            self.assertFalse(table['load_job_config']['autodetect'])

    def test_changed_input_does_not_replace_previous_package(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);handover=self.fixture(root);output=root/'prepared';output.mkdir()
            marker=output/'old.txt';marker.write_text('preserve')
            with (handover/'csv/coffee.csv').open('a') as f:f.write('changed')
            with self.assertRaisesRegex(ValueError,'checksum mismatch'):
                build_package(handover,output,'sample-project','landing','europe-west3')
            self.assertEqual(marker.read_text(),'preserve')

    def test_header_collisions_and_metadata_names_are_distinct(self):
        columns=['Price / kg','Price-kg','123','_fl_source_id','商品','PRICE_KG']
        mapping=field_names(columns)
        self.assertEqual(len(set(mapping.values())),len(columns))
        self.assertNotEqual(mapping['_fl_source_id'],'_fl_source_id')
        self.assertTrue(mapping['123'].startswith('field_'))

    def test_output_cannot_overwrite_the_handover(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);handover=self.fixture(root)
            with self.assertRaisesRegex(ValueError,'separate from'):
                build_package(handover,handover,'sample-project','landing','europe-west3')

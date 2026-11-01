"""Offline loader tests: no credentials, upload or live BigQuery calls."""
import json
import gzip
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.load_bigquery import run_cloud,validate_package,load_google_env
from src.prepare_bigquery import build_package
from tests import test_prepare_bigquery as fixtures


class FakeJob:
    def __init__(self,client,job_id,table,rows):
        self.client=client;self.job_id=job_id;self.destination=table
        self.job_type='load';self.state='DONE';self.error_result=None;self.output_rows=rows
    def result(self,timeout=None,**kwargs):return self


class FakeClient:
    def __init__(self):
        self.dataset=SimpleNamespace(location='europe-west3')
        self.tables={};self.jobs={};self.loads=0;self.creates=0;self.fail_after_commit=False
    def get_dataset(self,*args,**kwargs):return self.dataset
    def get_table(self,name,**kwargs):
        if name not in self.tables:raise NotFound('missing')
        return self.tables[name]
    def create_table(self,expected,**kwargs):
        self.creates+=1
        # BigQuery returns canonical INTEGER for the INT64 alias in our DDL.
        schema=[bigquery.SchemaField(f.name,'INTEGER' if f.field_type=='INT64' else f.field_type,mode=f.mode) for f in expected.schema]
        actual=SimpleNamespace(table_type='TABLE',schema=schema,time_partitioning=expected.time_partitioning,num_rows=0,snapshots=[])
        self.tables[str(expected.reference)]=actual
        return actual
    def get_job(self,job_id,**kwargs):
        if job_id not in self.jobs:raise NotFound('missing')
        return self.jobs[job_id]
    def load_table_from_file(self,stream,name,job_id,job_config,**kwargs):
        assert job_config.write_disposition=='WRITE_EMPTY'
        payload=stream.read()
        self.upload_is_gzip=payload.startswith(b'\x1f\x8b')
        if self.upload_is_gzip:payload=gzip.decompress(payload)
        rows=[json.loads(line) for line in payload.splitlines()]
        table=self.tables[name]
        if table.num_rows:raise ValueError('already populated')
        self.loads+=1;table.num_rows=len(rows);table.snapshots=[r['_fl_snapshot_id'] for r in rows]
        job=FakeJob(self,job_id,name,len(rows));self.jobs[job_id]=job
        if self.fail_after_commit:
            self.fail_after_commit=False
            raise ConnectionError('Simulated connection loss after the job committed')
        return job
    def query(self,sql,job_config,**kwargs):
        name=sql.split('FROM `')[1].split('`')[0]
        table=self.tables[name];snapshot=job_config.query_parameters[0].value
        return SimpleNamespace(result=lambda **_:iter([dict(total_rows=table.num_rows,snapshot_rows=table.snapshots.count(snapshot))]))


class LoaderTests(unittest.TestCase):
    def test_relative_credentials_work_in_a_shallow_checkout(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            root = Path(directory)
            key = root / 'key.json'
            key.write_text('{}')
            env = root / '.env'
            env.write_text('GOOGLE_APPLICATION_CREDENTIALS=key.json\n')
            with patch('src.load_bigquery.ROOT', Path('/checkout')):
                self.assertTrue(load_google_env(env))
            self.assertEqual(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], str(key))

    def test_shared_env_reads_only_google_settings_and_resolves_relative_key(self):
        with tempfile.TemporaryDirectory() as d,patch.dict(os.environ,{},clear=True):
            root=Path(d);(root/'key.json').write_text('{}')
            path=root/'.env';path.write_text('GOOGLE_APPLICATION_CREDENTIALS="key.json"\nBQ_PROJECT=sample-project # comment\nDATABASE_PASSWORD=ignored\n')
            self.assertTrue(load_google_env(path))
            self.assertEqual(os.environ['GOOGLE_APPLICATION_CREDENTIALS'],str(root/'key.json'))
            self.assertEqual(os.environ['BQ_PROJECT'],'sample-project')
            self.assertNotIn('DATABASE_PASSWORD',os.environ)

    def test_existing_google_environment_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as d,patch.dict(os.environ,{'BQ_PROJECT':'existing-project'},clear=True):
            path=Path(d)/'.env';path.write_text('BQ_PROJECT=another-project\n')
            load_google_env(path)
            self.assertEqual(os.environ['BQ_PROJECT'],'existing-project')

    def setup_package(self,root):
        handover=fixtures.BigQueryPreparationTests().fixture(root)
        package=root/'package'
        build_package(handover,package,'sample-project','landing','europe-west3')
        manifest,tables,checksum=validate_package(package)
        return package,manifest,tables,checksum

    def test_successful_rerun_does_not_submit_second_load(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);package,m,t,h=self.setup_package(root);client=FakeClient()
            first=run_cloud(m,t,h,client,'execute',root/'state')
            second=run_cloud(m,t,h,client,'execute',root/'state')
            self.assertEqual(first['tables'][0]['status'],'LOADED')
            self.assertEqual(second['tables'][0]['status'],'ALREADY_LOADED')
            self.assertEqual(client.loads,1)
            self.assertTrue(client.upload_is_gzip)

    def test_upload_ambiguity_recovers_cloud_job_on_rerun(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);package,m,t,h=self.setup_package(root);client=FakeClient();client.fail_after_commit=True
            first=run_cloud(m,t,h,client,'execute',root/'state')
            self.assertEqual(first['status'],'PARTIAL')
            second=run_cloud(m,t,h,client,'execute',root/'state')
            self.assertEqual(second['tables'][0]['status'],'ALREADY_LOADED')
            self.assertEqual(client.loads,1)

    def test_wrong_dataset_location_creates_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);package,m,t,h=self.setup_package(root);client=FakeClient();client.dataset.location='US'
            result=run_cloud(m,t,h,client,'execute',root/'state')
            self.assertEqual(result['status'],'FAILED')
            self.assertEqual(client.creates,0);self.assertEqual(client.loads,0)

    def test_check_mode_creates_and_loads_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);package,m,t,h=self.setup_package(root);client=FakeClient()
            result=run_cloud(m,t,h,client,'check',root/'state')
            self.assertEqual(result['tables'][0]['status'],'WOULD_CREATE_AND_LOAD')
            self.assertEqual(client.creates,0);self.assertEqual(client.loads,0)
            self.assertFalse((root/'state/load_history.json').exists())

    def test_populated_untracked_target_is_refused(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);package,m,t,h=self.setup_package(root);client=FakeClient()
            expected=bigquery.Table(t[0]['table'],schema=[bigquery.SchemaField.from_api_repr(f) for f in t[0]['schema']])
            expected.time_partitioning=bigquery.TimePartitioning(type_='DAY',field='_fl_prepared_at_utc')
            actual=client.create_table(expected);actual.num_rows=10
            result=run_cloud(m,t,h,client,'execute',root/'state')
            self.assertEqual(result['tables'][0]['status'],'FAILED');self.assertEqual(client.loads,0)
            self.assertEqual(actual.num_rows,10)

    def test_changed_local_data_fails_validation(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);package,m,t,h=self.setup_package(root)
            t[0]['data_path'].write_text('{}\n')
            with self.assertRaisesRegex(ValueError,'checksum mismatch'):validate_package(package)

    def test_schema_mismatch_does_not_change_target(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);package,m,t,h=self.setup_package(root);client=FakeClient()
            client.tables[t[0]['table']]=SimpleNamespace(table_type='TABLE',schema=[],num_rows=0,time_partitioning=None)
            result=run_cloud(m,t,h,client,'execute',root/'state')
            self.assertEqual(result['tables'][0]['status'],'FAILED');self.assertEqual(client.loads,0)
            self.assertEqual(client.tables[t[0]['table']].schema,[])

    def test_failed_table_does_not_stop_other_tables(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);package,m,t,h=self.setup_package(root);client=FakeClient()
            first=t[0]
            second={**first,'table':first['table']+'_second'}
            client.tables[first['table']]=SimpleNamespace(table_type='TABLE',schema=[],num_rows=0,time_partitioning=None)
            result=run_cloud(m,[first,second],h,client,'execute',root/'state')
            self.assertEqual([r['status'] for r in result['tables']],['FAILED','LOADED'])
            self.assertEqual(result['status'],'PARTIAL')

    def test_wrong_reconciled_count_is_not_recorded_as_success(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);package,m,t,h=self.setup_package(root);client=FakeClient()
            client.query=lambda *args,**kwargs:SimpleNamespace(result=lambda **_:iter([dict(total_rows=999,snapshot_rows=999)]))
            result=run_cloud(m,t,h,client,'execute',root/'state')
            self.assertEqual(result['tables'][0]['status'],'FAILED')
            history=json.loads((root/'state/load_history.json').read_text())
            self.assertTrue(all(r['status']!='LOADED' for r in history['loads'].values()))

# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Validate and load the prepared BigQuery package. The default is offline.

python -m src.load_bigquery                        # local validation only
python -m src.load_bigquery --check                # read cloud metadata only
python -m src.load_bigquery --execute              # create/load/reconcile
python -m src.load_bigquery --execute --scope prices

This is a first-load loader: existing populated targets must belong to this exact
recorded package. It never appends, truncates or alters existing table schemas.
"""
import argparse
import fcntl
import hashlib
import gzip
import json
import os
import re
import shlex
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from src.common.logging import get_logger
from src.common.source_config import DATA,ROOT

logger=get_logger(__name__)


def load_google_env(path):
    """Read only Google settings from the shared .env; never execute shell text."""
    path=Path(path).resolve()
    if not path.is_file():return False
    allowed={'GOOGLE_APPLICATION_CREDENTIALS','BQ_PROJECT','BQ_DATASET_LANDING','DBT_BIGQUERY_LOCATION'}
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        key,separator,value=line.strip().removeprefix('export ').partition('=')
        key=key.strip()
        if not separator or key not in allowed or key in os.environ:continue
        parts=shlex.split(value.strip(),comments=True)
        if not parts:continue
        if len(parts)!=1:raise ValueError(f'Use a quoted value for {key} in the shared env file.')
        value=parts[0]
        if key=='GOOGLE_APPLICATION_CREDENTIALS':
            credential=Path(value).expanduser()
            bases = [path.parent, ROOT.parent]
            if len(ROOT.parents) > 2:
                bases.append(ROOT.parents[2])
            candidates=[credential] if credential.is_absolute() else [
                base/credential for base in bases]
            found=list(dict.fromkeys(p.resolve() for p in candidates if p.is_file()))
            if not found:raise ValueError('Credential file configured in the shared env does not exist.')
            if len(found)>1:raise ValueError('Credential path is ambiguous; use an absolute path in the shared env.')
            value=str(found[0])
        os.environ[key]=value
    return True


def now():return datetime.now(timezone.utc).isoformat()


def save_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(value,indent=2)+'\n')
    temporary.replace(path)


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def package_file(root,name):
    path=(root/name).resolve()
    if not path.is_relative_to(root):raise ValueError('Package path escapes its folder.')
    return path


def validate_package(package,scope='all',selected=()):
    """Verify local records and schemas before constructing any cloud client."""
    package=Path(package).resolve()
    path=package/'load_manifest.json'
    manifest=json.loads(path.read_text())
    if manifest.get('status')!='PREPARED_NOT_LOADED':raise ValueError('Expected a prepared load manifest.')
    project=manifest['project'];dataset=manifest['dataset']
    if not re.fullmatch(r'[a-z][a-z0-9-]{4,61}[a-z0-9]',project):raise ValueError('Invalid project ID.')
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,1023}',dataset):raise ValueError('Invalid dataset ID.')
    if not re.fullmatch(r'[A-Za-z0-9-]+',manifest['location']):raise ValueError('Invalid dataset location.')
    names=set();tables=[];total=0;inputs=set()
    for spec in manifest['tables']:
        name=spec['table']
        prefix=f'{project}.{dataset}.'
        if not name.startswith(prefix) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,1023}',name[len(prefix):]):
            raise ValueError('Manifest destination does not match its project/dataset.')
        if name in names:raise ValueError('Duplicate target table in manifest.')
        names.add(name)
        data=package_file(package,spec['data_file']);schema_path=package_file(package,spec['schema_file'])
        if digest(data)!=spec['data_sha256'] or digest(schema_path)!=spec['schema_sha256']:
            raise ValueError(f'Package checksum mismatch: {name}')
        schema=json.loads(schema_path.read_text())
        columns={f['name']:f for f in schema}
        if len(columns)!=len(schema) or '_fl_snapshot_id' not in columns:
            raise ValueError('Schema requires distinct fields and snapshot metadata.')
        rows=0
        with data.open(encoding='utf-8') as stream:
            for line in stream:
                record=json.loads(line)
                if not isinstance(record,dict) or set(record)!=set(columns):
                    raise ValueError(f'Prepared record does not match schema: {name}')
                if record['_fl_snapshot_id']!=manifest['snapshot_id']:
                    raise ValueError('Record snapshot differs from manifest.')
                for key,field in columns.items():
                    value=record[key];kind=field['type']
                    if value is None:continue
                    if kind=='STRING' and not isinstance(value,str):raise ValueError('Non-string source value.')
                    if kind=='INT64' and (not isinstance(value,int) or isinstance(value,bool)):
                        raise ValueError('Invalid integer metadata.')
                    if kind=='TIMESTAMP':
                        if not isinstance(value,str) or datetime.fromisoformat(value.replace('Z','+00:00')).tzinfo is None:
                            raise ValueError('Timestamp metadata must include a timezone.')
                    if kind not in {'STRING','INT64','TIMESTAMP'}:raise ValueError('Unexpected prepared field type.')
                rows+=1
        if rows!=spec['expected_rows']:raise ValueError(f'Prepared row count mismatch: {name}')
        if rows!=sum(item['rows'] for item in spec['input_files']):raise ValueError('Input counts do not reconcile.')
        inputs.update(item['file'] for item in spec['input_files']);total+=rows
        counts=spec.get('review_counts',{})
        if scope=='prices' and set(counts)!={'PRICE_MAPPING_REVIEW'}:continue
        if selected and name not in selected and name[len(prefix):] not in selected:continue
        tables.append({**spec,'schema':schema,'data_path':data})
    if total!=manifest['raw_rows'] or len(inputs)!=manifest['input_files'] or len(names)!=manifest['proposed_tables']:
        raise ValueError('Prepared package totals do not reconcile.')
    requested=set(selected)
    for t in tables:
        requested.discard(t['table']);requested.discard(t['table'].rsplit('.',1)[-1])
    if requested:raise ValueError('Unknown table selection or outside selected scope: '+', '.join(sorted(requested)))
    if not tables:raise ValueError('No tables selected.')
    return manifest,tables,digest(path)


def schema_signature(fields):
    aliases={'INT64':'INTEGER','FLOAT64':'FLOAT','BOOL':'BOOLEAN'}
    return [(f.name,aliases.get(f.field_type,f.field_type),f.mode) for f in fields]


def check_table(actual,expected):
    if actual.table_type!='TABLE':raise ValueError('Target is not a native BigQuery table.')
    if schema_signature(actual.schema)!=schema_signature(expected.schema):
        raise ValueError('Existing table schema differs; no schema was changed.')
    partition=actual.time_partitioning
    if not partition or partition.field!='_fl_prepared_at_utc' or str(partition.type_)!='DAY':
        raise ValueError('Existing table partitioning differs; no table was changed.')


def job_base(spec):
    content=json.dumps([spec['table'],spec['data_sha256'],spec['schema_sha256']],separators=(',',':'))
    return 'farmlover_load_'+hashlib.sha256(content.encode()).hexdigest()[:40]


def find_job(client,spec,location,timeout):
    from google.api_core.exceptions import NotFound
    base=job_base(spec)
    # A failed BigQuery job is immutable. Retry with a deterministic new attempt;
    # recover a running or successful attempt before submitting any new upload.
    for attempt in range(1,101):
        job_id=base if attempt==1 else f'{base}_attempt_{attempt}'
        try:job=client.get_job(job_id,location=location,retry=None,timeout=timeout)
        except NotFound:return None,job_id
        if job.job_type!='load' or str(job.destination)!=spec['table']:
            raise ValueError('Recorded job does not match the expected load destination.')
        if job.state=='DONE' and job.error_result:continue
        return job,job_id
    raise ValueError('Too many failed attempts for this prepared table; inspect job history.')


def reconcile(client,spec,snapshot,location,request_timeout,job_timeout):
    from google.cloud import bigquery
    # Verify actual rows, not only table metadata or the job output statistic.
    sql=(f'SELECT COUNT(*) AS total_rows, '
         f'COUNTIF(_fl_snapshot_id = @snapshot) AS snapshot_rows FROM `{spec["table"]}`')
    config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('snapshot','STRING',snapshot)])
    job=client.query(sql,job_config=config,location=location,retry=None,job_retry=None,timeout=request_timeout)
    try:row=next(iter(job.result(retry=None,timeout=job_timeout)))
    except TimeoutError:
        # This is a read-only count query; a later rerun rechecks the load job.
        raise TimeoutError('Row reconciliation timed out; rerun to verify the existing load.')
    if row['total_rows']!=spec['expected_rows'] or row['snapshot_rows']!=spec['expected_rows']:
        raise ValueError(f'Loaded row reconciliation failed: total={row["total_rows"]}, snapshot={row["snapshot_rows"]}, expected={spec["expected_rows"]}.')
    return row['total_rows']


def run_cloud(manifest,tables,manifest_hash,client,mode,state_dir,request_timeout=15,job_timeout=60,upload_timeout=120):
    from google.api_core.exceptions import NotFound,Conflict
    from google.cloud import bigquery
    state_dir=Path(state_dir);history_path=state_dir/'load_history.json'
    history=json.loads(history_path.read_text()) if history_path.exists() else {'version':1,'loads':{}}
    if history.get('version')!=1 or not isinstance(history.get('loads'),dict):
        raise ValueError('Invalid saved load history; preserve it for investigation.')
    report=dict(started_at_utc=now(),mode=mode,status='IN_PROGRESS',project=manifest['project'],
                dataset=manifest['dataset'],location=manifest['location'],snapshot_id=manifest['snapshot_id'],
                manifest_sha256=manifest_hash,selected_tables=len(tables),expected_rows=sum(t['expected_rows'] for t in tables),
                permission_scope='Check mode reads dataset/table/job metadata; it does not prove create/load/query permissions.',tables=[])
    report_path=state_dir/('check_report.json' if mode=='check' else 'load_report.json')
    save_json(report_path,report)
    dataset_id=f'{manifest["project"]}.{manifest["dataset"]}'
    location=manifest['location']
    try:
        try:dataset=client.get_dataset(dataset_id,retry=None,timeout=request_timeout)
        except NotFound:
            dataset=None
            if mode=='execute':
                expected=bigquery.Dataset(dataset_id);expected.location=location
                dataset=client.create_dataset(expected,exists_ok=True,retry=None,timeout=request_timeout)
        if dataset is not None and dataset.location.lower()!=location.lower():
            raise ValueError(f'Dataset location is {dataset.location}; manifest expects {location}. Reprepare for the actual location.')
        report['dataset_status']='FOUND' if dataset is not None else 'WOULD_CREATE'
        for spec in tables:
            item=dict(table=spec['table'],expected_rows=spec['expected_rows'],status='IN_PROGRESS')
            report['tables'].append(item);save_json(report_path,report)
            try:
                expected=bigquery.Table(spec['table'],schema=[bigquery.SchemaField.from_api_repr(f) for f in spec['schema']])
                expected.time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,field='_fl_prepared_at_utc')
                expected.labels={'application':'farmlover','layer':'landing'}
                try:actual=client.get_table(spec['table'],retry=None,timeout=request_timeout)
                except NotFound:actual=None
                if actual is not None:check_table(actual,expected)
                job,job_id=find_job(client,spec,location,request_timeout)
                item['job_id']=job_id
                key=job_base(spec);saved=history['loads'].get(key)
                if mode=='check':
                    if actual is None:
                        item['status']='WOULD_CREATE_AND_LOAD'
                    elif job is not None or (saved and saved.get('status')=='LOADED'):
                        item['status']='EXISTING_JOB_OR_LOAD_TO_RECONCILE'
                    elif actual.num_rows:
                        raise ValueError('Target contains untracked rows; append and replacement are disabled.')
                    else:item['status']='EMPTY_TABLE_READY'
                    continue
                if actual is None:
                    actual=client.create_table(expected,exists_ok=True,retry=None,timeout=request_timeout)
                    check_table(actual,expected)
                already_loaded=saved and saved.get('status')=='LOADED'
                if job is None and not already_loaded:
                    # The service also enforces WRITE_EMPTY, covering concurrent writers.
                    if actual.num_rows:raise ValueError('Target contains untracked rows; append and replacement are disabled.')
                    config=bigquery.LoadJobConfig(schema=expected.schema,
                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                        write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
                        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
                        autodetect=False,ignore_unknown_values=False,max_bad_records=0)
                    history['loads'][key]=dict(status='SUBMITTED',job_id=job_id,table=spec['table'],
                        data_sha256=spec['data_sha256'],schema_sha256=spec['schema_sha256'],
                        snapshot_id=manifest['snapshot_id'],expected_rows=spec['expected_rows'],updated_at_utc=now())
                    save_json(history_path,history)
                    try:
                        with spec['data_path'].open('rb') as stream:
                            # Recheck immediately before upload; never replace or append.
                            if digest(spec['data_path'])!=spec['data_sha256']:raise ValueError('Prepared data changed before upload.')
                            # Gzip transport preserves decoded records and job identity.
                            with tempfile.TemporaryFile() as transport:
                                with gzip.GzipFile(filename='',fileobj=transport,mode='wb',mtime=0) as packed:
                                    shutil.copyfileobj(stream,packed)
                                item['upload_bytes']=transport.tell();item['upload_compression']='gzip'
                                transport.seek(0)
                                job=client.load_table_from_file(transport,spec['table'],job_id=job_id,location=location,
                                    job_config=config,num_retries=1,timeout=upload_timeout)
                    except Conflict:job=client.get_job(job_id,location=location,retry=None,timeout=request_timeout)
                    item['submitted_this_run']=True
                else:item['submitted_this_run']=False
                if job is not None:
                    job.result(retry=None,timeout=job_timeout)
                    if job.error_result:raise ValueError('BigQuery load job failed; inspect its job ID.')
                    if job.output_rows!=spec['expected_rows']:raise ValueError('Load job output count differs from manifest.')
                count=reconcile(client,spec,manifest['snapshot_id'],location,request_timeout,job_timeout)
                history['loads'][key]=dict(status='LOADED',job_id=job_id,table=spec['table'],
                    data_sha256=spec['data_sha256'],schema_sha256=spec['schema_sha256'],
                    manifest_sha256=manifest_hash,snapshot_id=manifest['snapshot_id'],rows=count,updated_at_utc=now())
                save_json(history_path,history)
                item.update(status='LOADED' if item['submitted_this_run'] else 'ALREADY_LOADED',loaded_rows=count)
            except TimeoutError as error:
                item.update(status='PENDING_VERIFICATION',error=str(error),next_action='Rerun the same command; do not regenerate the package. A submitted job may still finish.')
            except Exception as error:
                item.update(status='FAILED',error_type=type(error).__name__,error=str(error))
            finally:
                item['finished_at_utc']=now();save_json(report_path,report)
                logger.info('%s: %s',spec['table'],item['status'])
    except Exception as error:
        report.update(status='FAILED',error_type=type(error).__name__,error=str(error))
    except KeyboardInterrupt:
        report['status']='INTERRUPTED'
        report['next_action']='Rerun the same package to recover job state; active cloud jobs may finish.'
    finally:
        if report['status']=='IN_PROGRESS':
            report['status']='PARTIAL' if any(t['status'] in {'FAILED','PENDING_VERIFICATION'} for t in report['tables']) else 'FINISHED'
        report['finished_at_utc']=now()
        save_json(report_path,report)
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package',type=Path,default=DATA/'bigquery/latest')
    parser.add_argument('--env-file',type=Path,default=ROOT.parent/'data/local/.env',help='Shared Google settings; existing environment variables take precedence')
    mode=parser.add_mutually_exclusive_group()
    mode.add_argument('--check',action='store_true',help='Read cloud dataset/table/job metadata; create nothing')
    mode.add_argument('--execute',action='store_true',help='Create missing objects and run first loads')
    parser.add_argument('--scope',choices=['all','prices'],default='all',help='all raw containers (default) or recognised price/benchmark groups')
    parser.add_argument('--table',action='append',default=[],help='Select table names; repeat for several')
    parser.add_argument('--state-dir',type=Path,default=DATA/'bigquery')
    parser.add_argument('--request-timeout',type=float,default=15)
    parser.add_argument('--job-timeout',type=float,default=60)
    parser.add_argument('--upload-timeout',type=float,default=120,help='Seconds per upload request; metadata uses --request-timeout')
    args=parser.parse_args()
    if not 0<args.request_timeout<=300 or not 0<args.job_timeout<=3600:
        parser.error('Request timeout must be 0–300 seconds and job timeout 0–3600, both positive.')
    if not 0<args.upload_timeout<=3600:parser.error('Upload timeout must be positive and at most 3600 seconds.')
    manifest,tables,manifest_hash=validate_package(args.package,args.scope,args.table)
    logger.info('Validated package: %s tables, %s rows; target %s.%s (%s).',len(tables),
                sum(t['expected_rows'] for t in tables),manifest['project'],manifest['dataset'],manifest['location'])
    if not args.check and not args.execute:
        for t in tables:logger.info('PLAN %s: %s rows; %s',t['table'],t['expected_rows'],t['review_counts'])
        logger.info('Offline plan only. Use --check for cloud metadata or --execute to load.')
        return 0
    if load_google_env(args.env_file):logger.info('Read shared Google settings; credential values are not logged.')
    for key,value in [('BQ_PROJECT',manifest['project']),('BQ_DATASET_LANDING',manifest['dataset']),
                      ('DBT_BIGQUERY_LOCATION',manifest['location'])]:
        if os.environ.get(key) and os.environ[key].lower()!=value.lower():
            raise ValueError(f'{key} differs from the prepared manifest. Review the destination before loading.')
    from google.cloud import bigquery
    args.state_dir.mkdir(parents=True,exist_ok=True)
    with (args.state_dir/'load.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise ValueError('Another loader/check owns this state folder.')
        client=bigquery.Client(project=manifest['project'],location=manifest['location'])
        try:report=run_cloud(manifest,tables,manifest_hash,client,'execute' if args.execute else 'check',
                             args.state_dir,args.request_timeout,args.job_timeout,args.upload_timeout)
        finally:client.close()
    logger.info('Result: %s; reports and load history: %s',report['status'],args.state_dir)
    if report.get('error'):logger.error('%s',report['error'])
    for table in report['tables']:
        if table.get('error'):logger.error('%s: %s',table['table'],table['error'])
    return 0 if report['status']=='FINISHED' else 1


if __name__=='__main__':
    try:raise SystemExit(main())
    except Exception as error:
        logger.error('Loader could not start: %s',error)
        raise SystemExit(1)

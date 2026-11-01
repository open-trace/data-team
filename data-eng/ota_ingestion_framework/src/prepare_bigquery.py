# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Prepare saved handover CSVs for BigQuery without making cloud requests.

Run: python -m src.prepare_bigquery
Raw source fields remain STRING. Metadata has explicit types. The output includes
flat JSONL load files, schema JSON, DDL and complete file/column/load manifests.
No source records are dropped or deduplicated. Review status is not load approval.
"""
import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from src.common.logging import get_logger
from src.common.source_config import DATA
from src.review_collected_sources import inspect_export

logger=get_logger(__name__)
META=[
    ('_fl_source_id','STRING'),('_fl_source_url','STRING'),
    ('_fl_input_file','STRING'),('_fl_input_sha256','STRING'),
    ('_fl_source_report','STRING'),('_fl_collection_run_id','STRING'),
    ('_fl_retrieved_at_utc','TIMESTAMP'),('_fl_input_row_number','INT64'),
    ('_fl_table_label','STRING'),('_fl_data_kind','STRING'),
    ('_fl_review_status','STRING'),('_fl_snapshot_id','STRING'),
    ('_fl_prepared_at_utc','TIMESTAMP'),
]
READY={'MARKET_PRICE_ROWS','MARKET_PRICE_TABLE','BENCHMARK_QUOTES','COUNTY_PRICE_SUMMARIES'}
RELATED={'PRICE_TABLE_CANDIDATE','PRICE_INDEX_GRID','FORECAST_GRID',
         'TRADE_STATISTICS_CANDIDATE','TRADE_UNIT_VALUE_CANDIDATE','FOOD_PRICE_GRID'}
NOT_PRICE={'API_DOCUMENTATION','CATALOGUE_OR_SCHEDULE','MARKET_DIRECTORY',
           'CATALOGUE_METADATA','COMMODITY_CATALOGUE'}


def decision(category):
    if category in READY:return 'PRICE_MAPPING_REVIEW'
    if category in RELATED:return 'RELATED_MEASURE_REVIEW'
    if category.startswith('NON_COMMODITY') or category in NOT_PRICE:return 'OUTSIDE_PRICE_MODELS'
    return 'CONTENT_REVIEW'


def field_names(columns):
    """Map source headers to legal, unique BigQuery names without losing columns."""
    used={name.lower() for name,_ in META};result={}
    for index,column in enumerate(columns,1):
        if column in result:raise ValueError('Duplicate original CSV header.')
        ascii_name=unicodedata.normalize('NFKD',column).encode('ascii','ignore').decode()
        name=re.sub(r'[^A-Za-z0-9_]+','_',ascii_name).strip('_').lower() or f'column_{index}'
        if name[0].isdigit():name='field_'+name
        if name.startswith(('fl_','_file_','_table_','_partition','_row_timestamp','__root__','_colidentifier')):
            name='source_'+name
        name=name[:250];candidate=name;number=2
        while candidate.lower() in used:
            candidate=f'{name}_{number}';number+=1
        used.add(candidate.lower());result[column]=candidate
    return result


def group_name(item,category):
    """Only reviewed record families converge; other groups are raw containers."""
    sid=item['source_id']
    if category=='BENCHMARK_QUOTES' and sid.startswith('api_api_ninjas_com_v1_commodityprice_name_'):
        return 'farmlover_api_ninjas_quotes'
    if category=='MARKET_PRICE_ROWS' and sid=='za_durban_market_fresh':
        return 'farmlover_durban_prices'
    if category=='MARKET_PRICE_ROWS' and sid=='za_tshwane_market_fresh':
        return 'farmlover_tshwane_prices'
    if category=='MARKET_PRICE_ROWS' and sid=='za_cape_town_market_fresh':
        return 'farmlover_cape_town_prices'
    if category=='COUNTY_PRICE_SUMMARIES' and sid=='ke_strathmore_national_api':
        return 'farmlover_kenya_county_price_summaries'
    if category=='FOOD_PRICE_GRID':
        return 'farmlover_nbs_food_price_watch'
    if category=='MARKET_PRICE_TABLE' and sid=='mkulimahub_co_ke_market_prices':
        return 'farmlover_mkulimahub_market_prices'
    name=re.sub('[^a-z0-9_]+','_',f'farmlover_{sid}_{item["data_kind"]}'.lower())
    return name


def read_csv(path):
    with path.open(encoding='utf-8-sig',newline='') as stream:
        reader=csv.reader(stream,strict=True)
        columns=next(reader,None)
        if not columns or any(not c for c in columns) or len(columns)!=len(set(columns)):
            raise ValueError(f'CSV must have nonempty distinct headers: {path.name}')
        rows=[]
        for number,values in enumerate(reader,1):
            if len(values)!=len(columns):
                raise ValueError(f'CSV row {number} has {len(values)} fields; expected {len(columns)}: {path.name}')
            rows.append(dict(zip(columns,values)))
        return columns,rows


def csv_write(path,rows,columns=None):
    with path.open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=columns or list(rows[0]))
        writer.writeheader();writer.writerows(rows)


def build_package(handover,output,project,dataset,location):
    for value,pattern,label in [(project,r'[a-z][a-z0-9-]{4,61}[a-z0-9]','project'),
                                (dataset,r'[A-Za-z_][A-Za-z0-9_]{0,1023}','dataset'),
                                (location,r'[A-Za-z0-9-]+','location')]:
        if not re.fullmatch(pattern,value):raise ValueError(f'Invalid BigQuery {label}.')
    handover=Path(handover).resolve();output=Path(output).resolve()
    if output==handover or output.is_relative_to(handover) or handover.is_relative_to(output):
        raise ValueError('Preparation output must be separate from the handover.')
    manifest_path=handover/'export_report.json'
    manifest=json.loads(manifest_path.read_text())
    if manifest.get('status')!='FINISHED' or not manifest.get('files'):
        raise ValueError('A finished nonempty handover is required.')
    stamp=datetime.now(timezone.utc);snapshot=stamp.strftime('%Y%m%d_%H%M%S')
    temporary=output.parent/f'.preparing_{snapshot}'
    temporary.mkdir(parents=True,exist_ok=False)
    groups={};file_plan=[];column_plan=[];seen_files=set()
    try:
        # Verify every input first. No partial package is published on a bad file.
        for item in manifest['files']:
            relative=item['file'];path=(handover/relative).resolve()
            if not path.is_relative_to(handover) or relative in seen_files:
                raise ValueError('Duplicate or out-of-folder manifest file.')
            seen_files.add(relative)
            checksum=hashlib.sha256(path.read_bytes()).hexdigest()
            if checksum!=item['sha256']:raise ValueError(f'Input checksum mismatch: {relative}')
            columns,rows=read_csv(path)
            if len(rows)!=item['rows']:raise ValueError(f'Input row count mismatch: {relative}')
            reviewed=inspect_export(path,item);status=decision(reviewed['category'])
            name=group_name(item,reviewed['category'])
            group=groups.setdefault(name,dict(columns=[],files=[],rows=0))
            for c in columns:
                if c not in group['columns']:group['columns'].append(c)
            report=(handover/item['source_report']).resolve()
            if not report.is_relative_to(handover):raise ValueError('Report outside handover.')
            details=json.loads(report.read_text())
            entry={**item,'columns':columns,'review_category':reviewed['category'],
                   'review_status':status,'collection_run_id':details.get('run_id'),
                   'source_report_sha256':hashlib.sha256(report.read_bytes()).hexdigest()}
            group['files'].append(entry);group['rows']+=len(rows)
            file_plan.append(dict(input_file=relative,source_id=item['source_id'],source_url=item['source_url'],
                proposed_table=f'{project}.{dataset}.{name}',rows=len(rows),data_kind=item['data_kind'],
                review_category=reviewed['category'],review_status=status,input_sha256=checksum,
                collection_status=item.get('collection_status',''),reason=reviewed['reason']))
        total=sum(g['rows'] for g in groups.values())
        if len(file_plan)!=manifest['csv_files'] or total!=manifest['raw_rows']:
            raise ValueError('Handover totals do not reconcile with its manifest.')
        (temporary/'records').mkdir();(temporary/'schemas').mkdir()
        ddl=[f'-- Proposed landing tables only. No SQL was executed.\nCREATE SCHEMA IF NOT EXISTS `{project}.{dataset}`\nOPTIONS(location={json.dumps(location)});\n']
        table_plan=[];load_tables=[]
        for name,g in sorted(groups.items()):
            mapping=field_names(g['columns'])
            schema=[dict(name=value,type='STRING',mode='NULLABLE') for value in mapping.values()]
            schema += [dict(name=n,type=t,mode='NULLABLE') for n,t in META]
            if len(schema)>10000:raise ValueError(f'Too many columns: {name}')
            schema_path=f'schemas/{name}.json';data_path=f'records/{name}.jsonl'
            (temporary/schema_path).write_text(json.dumps(schema,indent=2)+'\n')
            written=0
            with (temporary/data_path).open('w',encoding='utf-8') as stream:
                for item in g['files']:
                    path=handover/item['file']
                    if hashlib.sha256(path.read_bytes()).hexdigest()!=item['sha256']:
                        raise ValueError('Input changed during preparation.')
                    columns,rows=read_csv(path)
                    for number,row in enumerate(rows,1):
                        record={mapping[c]:row.get(c) for c in g['columns']}
                        record.update(dict(_fl_source_id=item['source_id'],_fl_source_url=item['source_url'],
                            _fl_input_file=item['file'],_fl_input_sha256=item['sha256'],
                            _fl_source_report=item['source_report'],_fl_collection_run_id=item['collection_run_id'],
                            _fl_retrieved_at_utc=item.get('retrieved_at_utc'),_fl_input_row_number=number,
                            _fl_table_label=item['label'],_fl_data_kind=item['data_kind'],
                            _fl_review_status=item['review_status'],_fl_snapshot_id=snapshot,
                            _fl_prepared_at_utc=stamp.isoformat()))
                        stream.write(json.dumps(record,ensure_ascii=False,allow_nan=False)+'\n');written+=1
                    for c in columns:
                        column_plan.append(dict(input_file=item['file'],source_column=c,bigquery_column=mapping[c],
                                                proposed_table=name,type='STRING'))
            if written!=g['rows']:raise ValueError(f'Prepared count mismatch: {name}')
            fields=',\n  '.join(f'`{f["name"]}` {f["type"]}' for f in schema)
            ddl.append(f'CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{name}` (\n  {fields}\n)\nPARTITION BY DATE(`_fl_prepared_at_utc`);\n')
            decisions=Counter(f['review_status'] for f in g['files'])
            table_plan.append(dict(proposed_table=name,input_files=len(g['files']),rows=written,
                source_columns=len(mapping),review_status='; '.join(f'{k}: {v}' for k,v in decisions.items())))
            load_tables.append(dict(table=f'{project}.{dataset}.{name}',data_file=data_path,schema_file=schema_path,
                expected_rows=written,input_files=[dict(file=f['file'],sha256=f['sha256'],rows=f['rows'],
                    report_sha256=f['source_report_sha256']) for f in g['files']],
                data_sha256=hashlib.sha256((temporary/data_path).read_bytes()).hexdigest(),
                schema_sha256=hashlib.sha256((temporary/schema_path).read_bytes()).hexdigest(),
                review_counts=dict(decisions),load_job_config=dict(source_format='NEWLINE_DELIMITED_JSON',
                    autodetect=False,write_disposition='WRITE_EMPTY',create_disposition='CREATE_NEVER',
                    ignore_unknown_values=False,max_bad_records=0)))
        (temporary/'00_landing_tables.sql').write_text('\n'.join(ddl))
        csv_write(temporary/'table_plan.csv',table_plan)
        csv_write(temporary/'file_plan.csv',file_plan)
        csv_write(temporary/'column_mapping.csv',column_plan)
        result=dict(status='PREPARED_NOT_LOADED',prepared_at_utc=stamp.isoformat(),snapshot_id=snapshot,
            project=project,dataset=dataset,location=location,handover=str(handover),
            handover_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            input_files=len(file_plan),raw_rows=total,proposed_tables=len(groups),
            files_by_review_status=dict(Counter(f['review_status'] for f in file_plan)),tables=load_tables,
            scope='All raw CSV records preserved. Proposed landing containers are not business models. No cloud request, table creation, deduplication or dbt transformation occurred. Source values remain strings; blank cells remain empty strings and absent columns become null. Review status does not grant reporting approval.',
            load_safety='Initial loads require empty existing tables. A loader must verify schema, checksums, row counts and permissions, and persist a successful-load ledger before reruns. No append commands are generated.')
        (temporary/'load_manifest.json').write_text(json.dumps(result,indent=2)+'\n')
        (temporary/'README.md').write_text(f'''# BigQuery preparation\n\n{len(file_plan)} CSV inputs, {total:,} raw rows, {len(groups)} proposed landing tables. No cloud actions were performed.\n\nDestination proposal: `{project}.{dataset}`; location `{location}`. These values follow repository defaults and are not a verification of live dataset configuration.\n\nStart with table_plan.csv and file_plan.csv. The three reviewed record families group Durban prices, MkulimaHub market tables and API-Ninjas quotes. Other source/extraction-kind groups are raw containers; they may contain different sheets or page tables and need logical-dataset review before dbt models. OUTSIDE_PRICE_MODELS rows are retained for traceability but excluded from the price modelling shortlist. CONTENT_REVIEW and RELATED_MEASURE_REVIEW remain unresolved. Nothing has been automatically approved for a cloud load.\n\n## Files\n\n- records/: flat newline-delimited JSON load files with actual named columns; no payload wrapper.\n- schemas/: explicit BigQuery schemas. Source fields are STRING; metadata includes timestamps and row numbers.\n- 00_landing_tables.sql: proposed dataset/table DDL. Existing table schemas and location must be checked before applying it.\n- load_manifest.json: input/output checksums, row counts, review states and strict first-load settings.\n- column_mapping.csv: every original header and its legal BigQuery name.\n\nOriginal CSVs, duplicates and source values are preserved. Header rows inside workbook grids remain raw rows. Quoted newlines and Unicode are preserved in JSON strings. The source CSV format already lost distinctions between some missing/null/empty values; this preparation cannot reconstruct them. No date, currency, price or unit defaults were invented. Retrieval metadata is copied from the handover manifest; it may describe a run rather than each page. The preparation timestamp is separate. Original paths in source fields remain lineage, not portable cloud URLs.\n\n## Next step\n\nReview dataset selection, logical table groupings, allowed content, IAM and dataset location. Apply reviewed DDL and implement a reconciled load using this manifest, with a successful-load ledger. WRITE_EMPTY is proposed for first-load safety and will refuse a populated table. Do not switch to append for a retry without idempotency controls. Then define dbt typing, mappings, keys and reporting eligibility.\n''')
        output.parent.mkdir(parents=True,exist_ok=True)
        if output.exists():
            archive=DATA/'archive/bigquery_preparation'/snapshot
            archive.parent.mkdir(parents=True,exist_ok=True)
            shutil.move(str(output),str(archive))
        temporary.rename(output)
        return result
    except BaseException:
        shutil.rmtree(temporary,ignore_errors=True)
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project',default=os.environ.get('GCP_PROJECT_ID','opentrace-prod-5ga4'))
    parser.add_argument('--dataset',default='landing')
    parser.add_argument('--location',default='europe-west3')
    parser.add_argument('--input',type=Path,default=DATA/'exports/latest')
    parser.add_argument('--output',type=Path,default=DATA/'bigquery/latest')
    args=parser.parse_args()
    result=build_package(args.input,args.output,args.project,args.dataset,args.location)
    logger.info('Prepared %s input CSVs, %s rows and %s proposed tables: %s',
                result['input_files'],result['raw_rows'],result['proposed_tables'],args.output)
    logger.info('Review states: %s. No cloud data loaded.',result['files_by_review_status'])


if __name__=='__main__':
    try:main()
    except (OSError,ValueError,csv.Error) as error:
        logger.error('BigQuery preparation failed: %s',error)
        raise SystemExit(1)

# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Raw API collection with crop traversal, pagination and resumable progress.

API-specific routes/parameters must be supplied by a reviewed profile. Following
links is automatic only when actual next-link metadata exists. No endpoint guesses.
"""

import hashlib
import json
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, urljoin, urlsplit

from src.common.logging import get_logger
from src.common.source_config import ROOT, DATA, source_folder
from src.common.source_testing import Client, Evidence, ProbeError, tls_bundle
from src.common.record_output import export_source_jsonl_csv

logger = get_logger(__name__)


def at(value, path):
    for key in path.split('.') if path else []:
        if not isinstance(value, dict) or key not in value:
            raise ProbeError(f"Configured JSON path is missing: {path}")
        value = value[key]
    return value


def same_host(base, target):
    target = urljoin(base, target)
    if urlsplit(target).scheme != 'https' or urlsplit(target).netloc != urlsplit(base).netloc:
        raise ProbeError('API traversal requires HTTPS on the configured host.')
    return target


def save_json(path, value):
    temporary = path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    temporary.replace(path)


def rows_at(payload, path, record_shape='list'):
    rows = at(payload, path)
    if record_shape == 'object':
        if not isinstance(rows, dict) or not rows or 'error' in rows:
            raise ProbeError('Expected one nonempty JSON record object without an API error.')
        return [rows]
    if record_shape != 'list':
        raise ProbeError('Unsupported JSON record shape.')
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ProbeError('Expected a JSON list of record objects.')
    return rows


def iter_pages(request, base_url, spec, progress=None):
    """Yield saved pages plus the next checkpoint. Stop only on declared evidence.

    Modes: auto next-link discovery, next_link, page, offset, cursor, none.
    Page/offset modes require total_path or explicit terminal=empty/short_page.
    """
    paging = spec.get('pagination', {})
    mode = paging.get('mode', 'auto')
    if mode not in {'auto', 'next_link', 'page', 'offset', 'cursor', 'none'}:
        raise ProbeError('Unsupported API pagination mode.')
    state = progress or {'url': spec.get('url', base_url), 'params': dict(spec.get('params', {})), 'position': paging.get('start', 1 if mode == 'page' else 0), 'seen_pages': [], 'seen_requests': [], 'rows_seen': 0}
    size = paging.get('page_size', 1000)
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        raise ProbeError('API page_size must be a positive integer.')
    for _ in range(10000):
        url = same_host(base_url, state['url'])
        params = dict(state['params'])
        if mode in {'page', 'offset'}:
            parameter = paging.get('parameter')
            if not parameter or not (paging.get('total_path') or paging.get('terminal') in {'empty', 'short_page'}):
                raise ProbeError('Numbered pagination needs a parameter and a reviewed termination rule.')
            params[parameter] = state['position']
            if paging.get('size_parameter'):
                params[paging['size_parameter']] = size
        signature = hashlib.sha256(json.dumps([url, params], sort_keys=True).encode()).hexdigest()
        if signature in state['seen_requests']:
            raise ProbeError('API pagination repeated a request; stopping the loop.')
        payload = request(url, params)
        rows = rows_at(payload, spec.get('records_path', ''), spec.get('record_shape', 'list'))
        fingerprint = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()
        if rows and fingerprint in state['seen_pages']:
            raise ProbeError('API returned a repeated page; parameters may be ignored.')
        next_state = {**state, 'seen_requests': [*state['seen_requests'], signature],
                      'seen_pages': [*state['seen_pages'], fingerprint], 'rows_seen': state['rows_seen'] + len(rows)}
        complete, reason = False, ''
        try:
            if mode in {'auto', 'next_link'}:
                path = paging.get('next_path')
                if path is None and isinstance(payload, dict):
                    for candidate in ['links.next.href', 'links.next', 'pagination.next', 'next_page_url', 'next',
                                      'original.links.next', 'original.pagination.next', 'original.next_page_url', 'original.next']:
                        try:
                            value = at(payload, candidate)
                        except ProbeError:
                            continue
                        if value is None or isinstance(value, str):
                            path = candidate
                            break
                if path is None:
                    yield rows, None, False, 'No reviewed pagination contract or next-link metadata; coverage unknown.'
                    return
                link = at(payload, path)
                complete = link is None or link == ''
                if not complete:
                    if not isinstance(link, str):
                        raise ProbeError('Next-link metadata must contain a URL, not an inferred page number.')
                    next_state['url'] = same_host(url, link)
                    # Preserve filters when a returned relative link contains only paging.
                    query = parse_qs(urlsplit(next_state['url']).query)
                    for key, value in spec.get('params', {}).items():
                        if key in query and query[key] != [str(value)]:
                            raise ProbeError('Next link changes a configured API filter.')
                    next_state['params'] = {key: value for key, value in spec.get('params', {}).items() if key not in query}
            elif mode in {'page', 'offset'}:
                total = at(payload, paging['total_path']) if paging.get('total_path') else None
                if total is not None:
                    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
                        raise ProbeError('Declared API record total must be a nonnegative integer.')
                    if next_state['rows_seen'] > total:
                        raise ProbeError('Fetched rows exceed the declared record total.')
                    complete = next_state['rows_seen'] == total
                    if not rows and not complete:
                        raise ProbeError('API ended before its declared record total.')
                elif paging.get('terminal') == 'empty':
                    complete = not rows
                else:
                    complete = len(rows) < size
                next_state['position'] += 1 if mode == 'page' else len(rows)
            elif mode == 'cursor':
                token = at(payload, paging['next_path'])
                complete = token is None or token == ''
                if not complete:
                    if not isinstance(token, (str, int)) or isinstance(token, bool):
                        raise ProbeError('Unsupported pagination cursor type.')
                    next_state['params'][paging['parameter']] = token
            else:
                complete = True  # Explicit single-response contract, not source-wide coverage.
        except (ProbeError, KeyError, TypeError, ValueError) as error:
            detail = str(error) if isinstance(error, ProbeError) else type(error).__name__
            yield rows, None, False, "Pagination metadata needs review: " + detail
            return
        yield rows, None if complete else next_state, complete, reason
        if complete:
            return
        state = next_state
    raise ProbeError('API page safety limit reached; progress retained.')


def collect_api_source(source, *, max_seconds):
    source_id = source['source_id']
    source_folder(source_id)
    profile = source['profile']
    settings = profile.get('collection', {})
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    suffix = f'{source_id}_{run_id}'
    raw = DATA / 'raw' / source_id / run_id
    collected = DATA / 'collected' / source_id / run_id
    raw.mkdir(parents=True, exist_ok=False)
    collected.mkdir(parents=True, exist_ok=False)
    checkpoint_path = raw.parent / 'api_checkpoint.json'
    jsonl_path = collected / f'records_{suffix}.jsonl'
    csv_path = jsonl_path.with_suffix('.csv')
    report_path = raw / f'collection_report_{suffix}.json'
    evidence = Evidence(source)
    contract = {'url': source['source_url'], 'records_path': profile.get('records_path', ''),
                'record_shape': profile.get('record_shape', 'list'),
                'params': profile.get('params', {}), **settings}
    if 'pagination' in profile and 'pagination' not in settings:
        contract['pagination'] = profile['pagination']
    contract_hash = hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()
    report = {'source_id': source_id, 'run_id': run_id, 'source_url': source['source_url'],
              'started_at_utc': datetime.now(timezone.utc).isoformat(), 'status': 'IN_PROGRESS',
              'scope': settings.get('scope', 'Returned record lists; complete crop/market/history coverage unverified.'),
              'scope_completion': 'UNKNOWN', 'completeness': 'UNKNOWN', 'production_ready': False,
              'records_saved': 0, 'priced_records': 0, 'pages_fetched': 0, 'responses': [], 'failures': [],
              'csv_file': str(csv_path), 'records_file': str(jsonl_path), 'checkpoint_file': str(checkpoint_path),
              'resume_policy': 'At-least-once across runs; source dates can differ. Raw duplicates are retained.',
              'max_seconds': max_seconds}
    checkpoint = None
    if checkpoint_path.exists():
        previous = json.loads(checkpoint_path.read_text())
        if (previous.get('contract_hash') == contract_hash and not previous.get('finished')
                and (previous.get('pending') or not previous.get('catalogue_unverified'))):
            checkpoint = previous
    last_request = 0.0
    interrupted = None
    with jsonl_path.open('x', encoding='utf-8') as output:
        try:
            with tls_bundle(profile) as bundle:
                client = Client(evidence, verify=bundle, timeout=10, max_seconds=max_seconds, max_bytes=8_000_000)
                try:
                    def request(url, params=None):
                        nonlocal last_request
                        url = same_host(source['source_url'], url)
                        interval = float(settings.get('min_interval_seconds', 0.2))
                        delay = max(0, interval - (time.monotonic() - last_request))
                        if not 0 <= interval <= 60:
                            raise ProbeError('API minimum interval must be between zero and 60 seconds.')
                        if time.monotonic() + delay >= client.deadline:
                            raise ProbeError('Source time budget exhausted; pending API work retained.')
                        if delay:
                            time.sleep(delay)
                        name = f'api_response_{len(report["responses"]):06d}'
                        try:
                            body, _ = client.get(name, url, params=params)
                        finally:
                            last_request = time.monotonic()
                        path = raw / f'{name}_{suffix}.json'
                        path.write_bytes(body)
                        report['responses'].append({'file': str(path), 'sha256': hashlib.sha256(body).hexdigest(),
                                                    'retrieved_at_utc': datetime.now(timezone.utc).isoformat(), 'bytes': len(body)})
                        try:
                            payload = json.loads(body)
                        except (ValueError, UnicodeError):
                            raise ProbeError('API did not return valid JSON; raw response preserved.') from None
                        if isinstance(payload, dict) and (payload.get('success') is False or payload.get('error')):
                            raise ProbeError('API returned an application error; raw response preserved.')
                        return payload

                    if checkpoint is None:
                        checkpoint = {'contract_hash': contract_hash, 'series_started_at_utc': report['started_at_utc'],
                                      'stage': 'planning', 'tasks': [], 'pending': [], 'progress': {}, 'runs': [],
                                      'finished': False, 'crops_discovered': 0}
                        save_json(checkpoint_path, checkpoint)
                    if checkpoint.get('stage') in {'planning', 'crop_catalogue'}:
                        tasks = []
                        if settings.get('adapter') == 'strathmore':
                            catalogue = request(settings['options_url'])
                            crops, counties = catalogue.get('commodities'), catalogue.get('counties')
                            if not crops or not counties or not all(isinstance(v, str) and v for v in [*crops, *counties]):
                                raise ProbeError('Expected actual commodity and county option lists.')
                            crops, counties = list(dict.fromkeys(crops)), list(dict.fromkeys(counties))
                            crop_priority = settings.get('priority_commodities', [])
                            county_priority = settings.get('priority_counties', [])
                            crops = sorted(crops, key=lambda value: crop_priority.index(value) if value in crop_priority else len(crop_priority))
                            counties = sorted(counties, key=lambda value: county_priority.index(value) if value in county_priority else len(county_priority))
                            report.update(crops_discovered=len(crops), locations_discovered=len(counties))
                            # Exercise every commodity before moving to another
                            # county pair; priorities change order, never scope.
                            for index in range(0, len(counties), 2):
                                for crop in crops:
                                    selected = counties[index:index + 2]
                                    tasks.append({'url': settings['prices_url'], 'params': {'commodity': crop, 'countyA': selected[0],
                                                  'countyB': selected[1] if len(selected) > 1 else counties[0]},
                                                  'crop': crop, 'counties': selected, 'adapter': 'strathmore'})
                        else:
                            catalogue_spec = settings.get('crop_catalogue')
                            if catalogue_spec:
                                catalogue_spec = {**catalogue_spec, 'url': same_host(source['source_url'], catalogue_spec['url'])}
                                crop_values = checkpoint.get('crop_values', [])
                                catalogue_complete = False
                                checkpoint['stage'] = 'crop_catalogue'
                                save_json(checkpoint_path, checkpoint)
                                # Catalogue entries can be strings or objects; wrap strings for the common pager.
                                def catalogue_request(url, params):
                                    payload = request(url, params)
                                    values = at(payload, catalogue_spec.get('records_path', ''))
                                    if not isinstance(values, list):
                                        raise ProbeError('Crop catalogue is not a list.')
                                    converted = [{'value': value} if isinstance(value, (str, int)) and not isinstance(value, bool) else value for value in values]
                                    if any(not isinstance(v, dict) for v in converted):
                                        raise ProbeError('Unsupported crop catalogue entry.')
                                    # Use a new object so original raw JSON remains unchanged.
                                    return {'records': converted, 'original': payload}
                                paging = dict(catalogue_spec.get('pagination', {}))
                                for key in ['next_path', 'total_path']:
                                    if paging.get(key):
                                        paging[key] = 'original.' + paging[key]
                                spec = {**catalogue_spec, 'records_path': 'records', 'pagination': paging}
                                for entries, progress, complete, reason in iter_pages(catalogue_request, source['source_url'], spec, checkpoint.get('catalogue_progress')):
                                    for entry in entries:
                                        value = at(entry, catalogue_spec.get('id_field', 'value'))
                                        if isinstance(value, bool) or not isinstance(value, (str, int)):
                                            raise ProbeError('Crop identifiers must be strings or integers.')
                                        if value not in crop_values:
                                            crop_values.append(value)
                                    catalogue_complete = complete
                                    checkpoint.update(crop_values=crop_values, catalogue_progress=progress)
                                    save_json(checkpoint_path, checkpoint)
                                report['crop_catalogue_completion'] = 'COMPLETE' if catalogue_complete else 'UNKNOWN'
                                if not catalogue_complete:
                                    report['failures'].append({'stage': 'crop_catalogue', 'reason': 'Catalogue completeness unverified.'})
                                for value in crop_values:
                                    tasks.append({'url': settings.get('prices_url', source['source_url']),
                                                  'params': {**profile.get('params', {}), catalogue_spec['parameter']: value}, 'crop': value})
                            else:
                                tasks = [{'url': source['source_url'], 'params': profile.get('params', {})}]
                        checkpoint = {**checkpoint, 'stage': 'prices',
                                      'tasks': tasks, 'pending': list(range(len(tasks))), 'progress': {}, 'finished': False,
                                      'crops_discovered': len({t['crop'] for t in tasks if 'crop' in t}),
                                      'catalogue_unverified': bool(report.get('crop_catalogue_completion') == 'UNKNOWN')}
                        save_json(checkpoint_path, checkpoint)
                    report.update(resumed=bool(checkpoint['runs']), series_started_at_utc=checkpoint['series_started_at_utc'],
                                  crops_discovered=checkpoint['crops_discovered'], tasks_total=len(checkpoint['tasks']),
                                  tasks_pending_at_start=len(checkpoint['pending']))
                    pending_at_start = list(checkpoint['pending'])
                    for task_index in pending_at_start:
                        if time.monotonic() >= client.deadline:
                            break
                        task = checkpoint['tasks'][task_index]
                        try:
                            if task.get('adapter') == 'strathmore':
                                payload = request(task['url'], task['params'])
                                if payload.get('commodity') != task['crop']:
                                    raise ProbeError('API returned a different commodity from the requested crop.')
                                rows = []
                                for section, county in zip(['countyA', 'countyB'], task['counties']):
                                    record = payload.get(section)
                                    if not isinstance(record, dict) or record.get('county') != county:
                                        raise ProbeError('API returned a missing/mismatched county summary.')
                                    rows.append({**record, 'commodity': payload['commodity']})
                                pages = iter([(rows, None, True, '')])
                            else:
                                spec = {**task, 'records_path': profile.get('records_path', ''),
                                        'record_shape': profile.get('record_shape', 'list'),
                                        'pagination': settings.get('pagination', profile.get('pagination', {}))}
                                pages = iter_pages(request, source['source_url'], spec, checkpoint['progress'].get(str(task_index)))
                            finished = False
                            for rows, progress, complete, reason in pages:
                                for record in rows:
                                    if any(key in record for key in ['_response_file', '_collection_context']):
                                        raise ProbeError('Source record conflicts with collection metadata column names.')
                                    record = {**record, '_response_file': report['responses'][-1]['file'],
                                              '_collection_context': {'crop': task.get('crop'), 'counties': task.get('counties')}}
                                    output.write(json.dumps(record, ensure_ascii=False) + '\n')
                                    report['records_saved'] += 1
                                    report['priced_records'] += any(record.get(field) is not None for field in ['price', 'wholesalePrice', 'retailPrice', 'displayPrice'])
                                output.flush()
                                report['pages_fetched'] += 1
                                if complete:
                                    checkpoint['pending'].remove(task_index)
                                    checkpoint['progress'].pop(str(task_index), None)
                                    finished = True
                                elif progress:
                                    checkpoint['progress'][str(task_index)] = progress
                                else:
                                    report['failures'].append({'task': task_index, 'crop': task.get('crop'), 'reason': reason})
                                save_json(checkpoint_path, checkpoint)
                            if finished:
                                logger.info('%s: finished API task %s/%s; %s rows saved this run', source_id,
                                            len(checkpoint['tasks']) - len(checkpoint['pending']), len(checkpoint['tasks']), report['records_saved'])
                        except (ProbeError, ValueError, KeyError) as error:
                            report['failures'].append({'task': task_index, 'crop': task.get('crop'), 'reason': str(error) if isinstance(error, ProbeError) else type(error).__name__})
                            if isinstance(error, ProbeError) and ('time budget' in str(error).lower() or time.monotonic() >= client.deadline):
                                report['stopped_reason'] = 'Source time budget exhausted; remaining work retained.'
                                logger.info('%s: time budget reached; pending API work saved', source_id)
                                break
                            logger.warning('%s: task %s incomplete: %s', source_id, task_index, report['failures'][-1]['reason'])
                            if isinstance(error, ProbeError) and any(text in str(error) for text in ['HTTP 429', 'required retry wait', 'credentials required']):
                                report['stopped_reason'] = 'Rate limit, required cooldown or access restriction; remaining work retained.'
                                break
                    checkpoint['finished'] = not checkpoint['pending'] and not checkpoint.get('catalogue_unverified')
                    report['scope_completion'] = 'COMPLETE' if checkpoint['finished'] else 'PARTIAL'
                finally:
                    client.close()
        except BaseException as error:
            report['error'] = str(error) if isinstance(error, ProbeError) else type(error).__name__
            if not isinstance(error, Exception):
                interrupted = error
        finally:
            output.flush()
            if checkpoint is not None:
                checkpoint['runs'].append({'run_id': run_id, 'records_file': str(jsonl_path), 'rows': report['records_saved']})
                save_json(checkpoint_path, checkpoint)
                report.update(tasks_pending=len(checkpoint['pending']), series_runs=checkpoint['runs'])
    try:
        columns, count = export_source_jsonl_csv(jsonl_path, csv_path) if report['records_saved'] else ([], 0)
    except Exception as error:
        report.update(status='FAIL', csv_export_error=type(error).__name__, request_evidence=evidence.report)
        save_json(report_path, report)
        raise
    report.update(csv_columns=columns, csv_records=count, finished_at_utc=datetime.now(timezone.utc).isoformat(), request_evidence=evidence.report)
    report['status'] = 'FETCHED_SCOPE' if report['scope_completion'] == 'COMPLETE' and count else 'PARTIAL' if count or checkpoint and checkpoint.get('stage') == 'crop_catalogue' else 'FAIL'
    if not count:
        report['csv_file'] = None
    report['records_without_populated_price_fields'] = report['records_saved'] - report['priced_records']
    save_json(report_path, report)
    logger.info('%s: saved %s records; scope=%s; pending tasks=%s; CSV: %s', source_id, count,
                report['scope_completion'], report.get('tasks_pending', 'unknown'), csv_path)
    if interrupted is not None:
        raise interrupted
    return report['status'] == 'FETCHED_SCOPE'

# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Discover published download links and describe failed requests."""

from urllib.parse import unquote, urljoin, urlsplit

from src.common.source_testing import Page
from src.common.source_urls import clean_url


def download_links(body, page_url):
    """Return actual HTTPS CSV/XLSX/ZIP anchors, once per cleaned URL.

    Relative links are resolved against the final response URL. Cross-host links
    are retained because publishers commonly host downloads on a separate host.
    These are candidates; only the downloaded bytes establish their format.
    """
    found = {}
    for link in Page(body.decode('utf-8-sig', errors='replace')).links:
        target = clean_url(urljoin(page_url, link['href']))
        if not target or urlsplit(target).scheme != 'https':
            continue
        extension = unquote(urlsplit(target).path).lower().rsplit('.', 1)[-1]
        if extension in {'csv', 'xlsx', 'zip'}:
            found.setdefault(target, {'url': target, 'label': ' '.join(link['text']),
                                      'advertised_format': extension})
    return list(found.values())


def access_failure(endpoint):
    """Give a failed request an action without bypassing access controls."""
    status = endpoint.get('http_status')
    error = endpoint.get('error', '').lower()
    if endpoint.get('tls') == 'FAIL':
        code, retry, action = 'TLS_FAILED', False, 'Review the certificate chain and trusted CA configuration; keep verification enabled.'
    elif status in {401, 403}:
        code, retry, action = 'ACCESS_RESTRICTED', False, 'Check publisher access requirements or a documented public export; do not repeat blocked requests automatically.'
    elif status == 404:
        code, retry, action = 'NOT_FOUND', False, 'Find a current link on the publisher website.'
    elif status == 429 or 'required retry wait' in error:
        code, retry, action = 'COOLDOWN_REQUIRED', True, 'Retry later after the publisher cooldown; retain unfinished work.'
    elif status in {502, 503, 504}:
        code, retry, action = 'SERVER_UNAVAILABLE', True, 'Retry later with a bounded request budget.'
    elif 'byte limit' in error:
        code, retry, action = 'SIZE_LIMIT', False, 'Review the file size and configure a suitable limit before retrying.'
    elif status == 200:
        code, retry, action = 'DOWNLOAD_INTERRUPTED', True, 'The server responded but the body did not finish; retry the download within a bounded budget.'
    elif 'dns' in error:
        code, retry, action = 'DNS_FAILED', True, 'Check hostname resolution and retry later.'
    elif 'timed out' in error or 'time budget' in error or 'connection failed' in error:
        code, retry, action = 'CONNECTION_FAILED', True, 'Retry later with a bounded request budget; keep earlier successful files.'
    else:
        code, retry, action = 'REQUEST_NEEDS_REVIEW', False, 'Inspect the request error and publisher requirements before retrying.'
    return {'failure_code': code, 'retryable': retry, 'next_action': action}

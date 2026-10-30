# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Read-only HTTP, HTML and record checks shared by source probes.

Responses and records stay in memory. Reports contain counts, field names and
dates, never price records, cookies, hidden form values or signed query strings.
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import os
import re
import ssl
import subprocess
import tempfile
import time
from src.common.credentials import request_headers
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests


PROJECT_DIR = Path(__file__).resolve().parents[2]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def display_url(url: str) -> str:
    """Remove credentials, fragments and possibly signed query values."""
    p = urlsplit(url)
    host = p.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    if p.port:
        host += f":{p.port}"
    return urlunsplit((p.scheme, host, p.path, "REDACTED" if p.query else "", ""))


def secure_url(url: str) -> str:
    p = urlsplit(url)
    if p.scheme != "https" or not p.hostname or p.username or p.password:
        raise ProbeError("Only HTTPS URLs without embedded credentials are allowed.")
    return url


def normalise(value) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def numeric(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        text = str(value).strip().replace(",", "")
        text = re.sub(r"^(?:R|ZAR|KES|NGN|USD|KSh|₦|\$)\s*", "", text, flags=re.I)
        result = float(text)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def observation_date(value) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%b/%Y", "%d/%B/%Y", "%d %B %Y", "%d %b %Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T.+", text):
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError:
            pass
    return None


class ProbeError(Exception):
    """A safe diagnostic message with no response body or credentials."""


def retry_after_seconds(value: str) -> float | None:
    """Read either standard Retry-After format without logging its raw value."""
    try:
        value = value.strip()
        if re.fullmatch(r"[0-9]+", value):
            delay = float(value)
            return delay if math.isfinite(delay) else None
        retry_at = parsedate_to_datetime(value)
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
    except (ValueError, TypeError, OverflowError):
        return None


@dataclass
class Row:
    cells: list[str] = field(default_factory=list)
    links: list[str] = field(default_factory=list)


class Page(HTMLParser):
    """Read actual tables, inputs and forms, including nested ASP.NET tables."""

    def __init__(self, text: str):
        super().__init__(convert_charrefs=True)
        self.tables: list[list[Row]] = []
        self.forms: list[dict] = []
        self.links: list[dict] = []
        self.scripts: list[str] = []
        self.text: list[str] = []
        self.title: list[str] = []
        self._tables: list[int] = []
        self._rows: list[tuple[int, Row]] = []
        self._cells: list[tuple[Row, list[str]]] = []
        self._form = None
        self._select = None
        self._option = None
        self._anchor = None
        self._in_title = False
        self._skip = 0
        self.feed(text)

    @property
    def rows(self) -> list[Row]:
        return [row for table in self.tables for row in table]

    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        if tag in {"script", "style"}:
            self._skip += 1
            if tag == "script" and d.get("src"):
                self.scripts.append(d["src"])
        if tag == "title":
            self._in_title = True
        if tag == "table":
            self.tables.append([])
            self._tables.append(len(self.tables) - 1)
        if tag == "tr" and self._tables:
            self._rows.append((self._tables[-1], Row()))
        if tag in {"td", "th"} and self._rows:
            self._cells.append((self._rows[-1][1], []))
        if tag == "form":
            self._form = {"action": d.get("action", ""), "method": d.get("method", "get").upper(), "inputs": [], "selects": []}
            self.forms.append(self._form)
        if tag == "input":
            if self._form is not None:
                self._form["inputs"].append(d)
            if d.get("type", "text").lower() != "hidden" and self._cells:
                self._cells[-1][1].append(d.get("value", ""))
        if tag == "select":
            self._select = {"name": d.get("name", ""), "id": d.get("id", ""),
                            "multiple": "multiple" in d, "options": []}
            if self._form is not None:
                self._form["selects"].append(self._select)
        if tag == "option" and self._select is not None:
            self._option = {"value": d.get("value"), "selected": "selected" in d, "text": []}
            self._select["options"].append(self._option)
        if tag == "a":
            self._anchor = {"href": d.get("href", ""), "text": []}
            self.links.append(self._anchor)
            if self._rows:
                self._rows[-1][1].links.append(d.get("href", ""))

    def handle_data(self, text):
        if self._skip:
            return
        text = text.strip()
        if not text:
            return
        self.text.append(text)
        if self._in_title:
            self.title.append(text)
        if self._cells:
            self._cells[-1][1].append(text)
        if self._option is not None:
            self._option["text"].append(text)
        if self._anchor is not None:
            self._anchor["text"].append(text)

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self._skip = max(0, self._skip - 1)
        if tag == "title":
            self._in_title = False
        if tag in {"td", "th"} and self._cells:
            row, pieces = self._cells.pop()
            row.cells.append(" ".join(pieces).strip())
        if tag == "tr" and self._rows:
            table, row = self._rows.pop()
            self.tables[table].append(row)
        if tag == "table" and self._tables:
            self._tables.pop()
        if tag == "form":
            self._form = None
        if tag == "option":
            self._option = None
        if tag == "select":
            self._select = self._option = None
        if tag == "a":
            self._anchor = None

    def form(self, field_name: str) -> dict:
        for form in self.forms:
            names = [x.get("name") for x in form["inputs"] + form["selects"]]
            if field_name in names:
                return form
        raise ProbeError(f"Expected form field is missing: {field_name}.")


def form_data(form: dict) -> dict[str, str]:
    data = {}
    for item in form["inputs"]:
        if item.get("name") and item.get("type", "text").lower() not in {"submit", "button", "checkbox", "radio", "file"}:
            data[item["name"]] = item.get("value", "")
    for select in form["selects"]:
        options = select["options"]
        if select["name"] and options:
            selected = next((o for o in options if o["selected"]), options[0])
            data[select["name"]] = selected["value"] if selected["value"] is not None else " ".join(selected["text"])
    return data


def table_records(page: Page, required: list[str]) -> tuple[list[str], list[dict]]:
    wanted = {normalise(x) for x in required}
    for table in page.tables:
        for index, row in enumerate(table):
            keys = [normalise(x) for x in row.cells]
            if wanted.issubset(keys):
                records = [dict(zip(keys, r.cells)) for r in table[index + 1:] if len(r.cells) == len(keys) and r.cells != row.cells]
                return row.cells, records
    raise ProbeError("Expected data-table headers are missing: " + ", ".join(required) + ".")


class Evidence:
    def __init__(self, source: dict, emit=None):
        self.report = {"source_id": source["source_id"], "source_name": source["display_name"], "checked_at": utc_now(), "endpoints": [], "checks": [], "data_access": "NOT_TESTED", "production_ready": False}
        self.emit = emit or (lambda event: None)

    def endpoint(self, item):
        self.report.pop("active_request", None)
        self.report["endpoints"].append(item)
        self.emit({"kind": "endpoint", "value": item})

    def pending(self, item):
        self.report["active_request"] = dict(item)
        self.emit({"kind": "pending", "value": dict(item)})

    def check(self, name, status, detail, **metadata):
        item = {"name": name, "status": status, "detail": detail, **metadata}
        self.report["checks"].append(item)
        if name.endswith("records") and status == "PASS":
            self.report["data_access"] = "PASS"
        self.emit({"kind": "check", "value": item})


class Client:
    """Bounded, verified HTTPS with explicit redirects and safe retries."""

    def __init__(self, evidence: Evidence, *, timeout=10, max_seconds=60, max_bytes=20_000_000, verify=True, session=None, full_response=False):
        self.evidence = evidence
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "FarmLoverSourceTester/0.2"})
        self.timeout = timeout
        self.deadline = time.monotonic() + max_seconds
        self.max_bytes = max_bytes
        if verify is False:
            raise ProbeError("TLS verification cannot be disabled.")
        self.verify = verify
        self.full_response = full_response

    def close(self):
        self.session.close()

    def get(self, name, url, **kwargs):
        return self.request(name, "GET", url, **kwargs)

    def request(self, name, method, url, *, data=None, params=None, read_limit=None):
        start = time.monotonic()
        item = {"name": name, "method": method, "url": display_url(url), "status": "FAIL", "http_status": None, "tls": "NOT_TESTED", "redirects": [], "attempts": 0}
        current, current_method = url, method
        self.evidence.pending(item)
        if self.full_response:
            read_limit = None
        try:
            secure_url(current)
            for hop in range(6):
                secure_url(current)
                response = None
                for attempt in range(2):
                    remaining = self.deadline - time.monotonic()
                    if remaining <= 0:
                        raise ProbeError("Source time budget exhausted.")
                    item["attempts"] += 1
                    # TLS and HTTP results belong to this destination/attempt,
                    # not to a previous redirect host or a previous retry.
                    item.update(tls="NOT_TESTED", http_status=None, request_url=display_url(current))
                    item.pop("content_type", None)
                    self.evidence.pending(item)
                    try:
                        response = self.session.request(current_method, current, headers=request_headers(current), data=data if current_method == "POST" else None, params=params if hop == 0 else None, timeout=(min(5, self.timeout, remaining), min(self.timeout, remaining)), allow_redirects=False, stream=True, verify=self.verify)
                    except requests.exceptions.SSLError:
                        raise
                    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                        if current_method == "GET" and attempt == 0:
                            continue
                        raise
                    item["tls"] = "PASS"
                    item["http_status"] = response.status_code
                    self.evidence.pending(item)
                    if current_method == "GET" and response.status_code in {429, 502, 503, 504} and attempt == 0:
                        retry_after = response.headers.get("Retry-After")
                        delay = retry_after_seconds(retry_after) if retry_after is not None else 0.25
                        response.close()
                        if delay is None:
                            raise ProbeError(f"HTTP {response.status_code}: invalid Retry-After; request was not retried.")
                        remaining = self.deadline - time.monotonic()
                        if delay > min(5.0, self.timeout) or delay >= remaining:
                            raise ProbeError(f"HTTP {response.status_code}: required retry wait exceeds the time budget or five-second wait limit; request was not retried.")
                        item["retry_wait_seconds"] = delay
                        time.sleep(delay)
                        continue
                    break
                if response is None:
                    raise ProbeError("No HTTP response.")
                with response:
                    item["content_type"] = response.headers.get("Content-Type", "")
                    if response.status_code in {301, 302, 303, 307, 308}:
                        target = urljoin(current, response.headers.get("Location", ""))
                        item["redirects"].append({"status": response.status_code, "url": display_url(target)})
                        secure_url(target)
                        if current_method == "POST":
                            if urlsplit(current).netloc != urlsplit(target).netloc:
                                raise ProbeError("Cross-host POST redirect was refused.")
                            if response.status_code in {301, 302, 303}:
                                current_method, data = "GET", None
                        current, params = target, None
                        continue
                    if response.status_code in {401, 403}:
                        raise ProbeError(f"HTTP {response.status_code}: access denied or credentials required.")
                    if response.status_code != 200:
                        raise ProbeError(f"HTTP {response.status_code}: data request did not succeed.")
                    length = response.headers.get("Content-Length", "")
                    if length.isdigit() and int(length) > self.max_bytes and read_limit is None:
                        raise ProbeError("Response exceeds the configured byte limit; data was not fully checked.")
                    chunks, size, complete = [], 0, True
                    for chunk in response.iter_content(8192):
                        if time.monotonic() > self.deadline:
                            raise ProbeError("Source time budget exhausted while reading the response.")
                        size += len(chunk)
                        if size > self.max_bytes:
                            raise ProbeError("Response exceeds the configured byte limit; data was not fully checked.")
                        chunks.append(chunk)
                        if read_limit is not None and size >= read_limit:
                            complete = bool(length.isdigit() and int(length) == size and not response.headers.get("Content-Encoding"))
                            break
                    body = b"".join(chunks)
                    item.update(status="PASS", bytes_read=size, complete=complete, sampled=not complete, final_url=display_url(current))
                    item["elapsed_seconds"] = round(time.monotonic() - start, 2)
                    self.evidence.endpoint(item)
                    return body, current
            raise ProbeError("Redirect limit reached.")
        except requests.exceptions.SSLError:
            item["tls"] = "FAIL"
            item["error"] = "TLS certificate/hostname verification or handshake failed; verification remained enabled."
        except requests.exceptions.Timeout:
            item["error"] = "Connection/read timed out."
        except requests.exceptions.ConnectionError as exc:
            text = str(exc).lower()
            item["error"] = "DNS resolution failed." if any(x in text for x in ("nameresolution", "name resolution", "failed to resolve")) else "Connection failed."
        except requests.RequestException:
            item["error"] = "HTTP request failed."
        except ProbeError as exc:
            item["error"] = str(exc)
        item["elapsed_seconds"] = round(time.monotonic() - start, 2)
        from src.common.source_access import access_failure
        item.update(access_failure(item))
        self.evidence.endpoint(item)
        raise ProbeError(item["error"])


def tls_bundle(profile, base_bundle=None):
    """Return a context manager containing only independently validated CAs."""
    from contextlib import contextmanager

    @contextmanager
    def bundle():
        base = str(base_bundle or os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("CURL_CA_BUNDLE") or requests.certs.where())
        if not Path(base).is_file():
            raise ProbeError("The configured CA bundle file does not exist.")
        ssl.create_default_context(cafile=base)
        certificates = []
        for spec in profile.get("intermediates", []):
            path = (PROJECT_DIR / "config" / spec["file"]).resolve()
            if not path.is_relative_to((PROJECT_DIR / "config").resolve()):
                raise ProbeError("Intermediate certificate path must be inside config/.")
            text = path.read_text(encoding="ascii")
            digest = hashlib.sha256(ssl.PEM_cert_to_DER_cert(text)).hexdigest()
            if digest.lower() != spec["sha256"].lower():
                raise ProbeError("Intermediate certificate fingerprint differs from the reviewed certificate.")
            try:
                result = subprocess.run(["openssl", "verify", "-CAfile", base, str(path)], capture_output=True, timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                raise ProbeError("OpenSSL is required to validate the configured intermediate certificate.") from None
            if result.returncode:
                raise ProbeError("Intermediate certificate does not validate against the existing trusted roots.")
            certificates.append(text)
        if not certificates:
            yield base
            return
        with tempfile.TemporaryDirectory(prefix="farmlover_source_tls_") as folder:
            output = Path(folder) / "ca-bundle.pem"
            output.write_bytes(Path(base).read_bytes() + b"\n" + "\n".join(certificates).encode("ascii"))
            yield str(output)

    return bundle()


def check_records(evidence, name, records, *, date_field, price_fields, commodity_field, location_field=None, max_age_days=None, required_locations=None, unit_field=None, date_override=None, complete=True):
    """Check original values in memory; every listed price field is required."""
    required = [date_field, commodity_field, *price_fields]
    if date_override is not None:
        required.remove(date_field)
    if not records:
        evidence.check(name + ".records", "FAIL", "No data records returned.", record_count=0)
        return
    absent = sorted({key for row in records for key in required if key not in row})
    if absent:
        evidence.check(name + ".records", "FAIL", "Required record fields missing: " + ", ".join(absent))
        return
    dates, pairs, locations, commodities = [], {}, set(), set()
    invalid, positive, units_missing, duplicates = 0, 0, 0, 0
    seen = set()
    for row in records:
        fingerprint = hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).digest()
        duplicates += fingerprint in seen
        seen.add(fingerprint)
        observed = observation_date(date_override if date_override is not None else row.get(date_field))
        values = [numeric(row.get(key)) for key in price_fields]
        commodity = str(row.get(commodity_field) or "").strip()
        location = str(row.get(location_field) or "").strip() if location_field else "source market"
        if not observed or not commodity or not values or any(v is None or v < 0 for v in values):
            invalid += 1
            continue
        positive += any(v is not None and v > 0 for v in values)
        dates.append(observed)
        commodities.add(commodity)
        locations.add(location)
        key = (location, commodity)
        pairs[key] = max(observed, pairs.get(key, observed))
        if unit_field and not str(row.get(unit_field) or "").strip():
            units_missing += 1
    evidence.check(name + ".records", "PASS" if positive else "FAIL", "Original record fields, dates and numeric prices inspected in memory.", record_count=len(records), valid_rows=len(dates), invalid_rows=invalid, positive_price_rows=positive, fields=sorted(records[0]), earliest_observation=min(dates).isoformat() if dates else None, latest_observation=max(dates).isoformat() if dates else None, commodity_count=len(commodities), location_count=len(locations), complete_response=complete)
    evidence.check(name + ".validity", "FAIL" if invalid else "PASS", "Each required price must be numeric and nonnegative; invalid dates, commodities and prices are counted separately.", invalid_rows=invalid)
    if not complete:
        evidence.check(name + ".completeness", "PARTIAL", "Only part of the source result was inspected; uninspected rows or pages may contain other dates and locations.")
    evidence.check(name + ".duplicates", "FAIL" if duplicates else "PASS", "Exact duplicate rows in this response.", duplicate_count=duplicates)
    if unit_field:
        evidence.check(name + ".units", "FAIL" if units_missing else "PASS", "Source unit labels checked; no conversion applied.", missing_units=units_missing)
    else:
        evidence.check(name + ".units", "NOT_TESTED", "Price basis/currency and unit semantics still require the source dictionary.", required=False)
    if required_locations:
        matches = {label: any(normalise(loc) in {normalise(v) for v in aliases} for loc in locations) for label, aliases in required_locations.items()}
        evidence.check(name + ".coverage", "PASS" if all(matches.values()) else "FAIL" if complete else "PARTIAL", "Required locations checked against location fields, never commodity names; absence from a sample does not prove absence from the source.", locations_present=matches)
    if max_age_days is None:
        evidence.check(name + ".freshness", "NOT_TESTED", "No freshness threshold selected for this historical/context source.", required=False)
    elif pairs:
        today = datetime.now(timezone.utc).date()
        stale = sum((today - observed).days > max_age_days for observed in pairs.values())
        future = sum(observed > today for observed in pairs.values())
        evidence.check(name + ".freshness", "PARTIAL" if not complete else "FAIL" if stale or future else "PASS", "Dates checked separately for each returned commodity/location pair; sampled dates are not claimed to be the source's latest dates.", max_age_days=max_age_days, pairs_checked=len(pairs), stale_pairs=stale, future_pairs=future)
    else:
        evidence.check(name + ".freshness", "NOT_TESTED", "No valid source observation dates available.")


def dotted(value, path):
    for key in path.split(".") if path else []:
        if not isinstance(value, dict) or key not in value:
            raise ProbeError("Expected JSON object/path is missing.")
        value = value.get(key)
    return value


def json_pages(client, evidence, name, url, *, records_path, next_path, max_pages=3):
    """Return (records, complete) after following a declared next-link API."""
    if max_pages < 1:
        raise ProbeError("Pagination requires a positive page limit.")
    all_records, seen_urls, seen_pages = [], set(), set()
    current = url
    for page in range(1, max_pages + 1):
        if current in seen_urls:
            raise ProbeError("Pagination repeated an already visited URL.")
        seen_urls.add(current)
        body, final = client.get(f"{name}.page_{page}", current)
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeError):
            raise ProbeError("Expected JSON; received an error page or unsupported response.") from None
        records = dotted(payload, records_path)
        if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
            raise ProbeError("Expected a JSON record list.")
        fingerprint = hashlib.sha256(json.dumps(records, sort_keys=True).encode()).digest()
        if records and fingerprint in seen_pages:
            raise ProbeError("Pagination returned a repeated page of records.")
        seen_pages.add(fingerprint)
        all_records.extend(records)
        next_link = dotted(payload, next_path)
        if next_link is None or next_link == "":
            evidence.check(name + ".pagination", "PASS", "Declared next-link exhausted.", pages_checked=page, complete=True)
            return all_records, True
        if not isinstance(next_link, str):
            raise ProbeError("Pagination next link has an unsupported format.")
        current = secure_url(urljoin(final, next_link))
        if urlsplit(current).netloc != urlsplit(url).netloc:
            raise ProbeError("Cross-host pagination link requires an explicitly reviewed profile.")
    evidence.check(name + ".pagination", "PARTIAL", "Page limit reached; remaining pages were not tested.", pages_checked=max_pages, complete=False)
    return all_records, False

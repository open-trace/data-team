# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Bounded raw CSV/Excel exports; preserve workbook grids for later mapping."""

import csv
import io
import time
import zipfile
from datetime import date, datetime
from itertools import islice

import openpyxl
from openpyxl.utils import get_column_letter

from src.common.source_testing import ProbeError


def _archive(body, byte_limit):
    archive = zipfile.ZipFile(io.BytesIO(body))
    if sum(info.file_size for info in archive.infolist()) > byte_limit * 5:
        archive.close()
        raise ProbeError("Uncompressed workbook/archive exceeds the inspection limit.")
    return archive


def tabular_sheets(body, kind, *, deadline, byte_limit=8_000_000, row_limit=10000):
    """Yield (label, records, truncated). Excel columns use A/B/C, including header rows.

    Export at most ten sheet/file selections and row_limit rows per selection.
    Original bytes remain the authoritative copy. These are not conformed prices.
    """
    def check_time():
        if time.monotonic() >= deadline:
            raise ProbeError("Source time budget exhausted while exporting tables.")

    def scalar(value):
        return value.isoformat() if isinstance(value, (date, datetime)) else value

    def workbook_sheets(payload, prefix):
        with _archive(payload, byte_limit):
            pass
        workbook = openpyxl.load_workbook(io.BytesIO(payload), read_only=True, data_only=True, keep_links=False)
        try:
            for sheet in workbook.worksheets:
                rows, truncated = [], False
                for index, values in enumerate(sheet.iter_rows(values_only=True), 1):
                    check_time()
                    if not any(value is not None for value in values):
                        continue
                    if len(rows) >= row_limit:
                        truncated = True
                        break
                    rows.append({"excel_row": index, **{get_column_letter(column): scalar(value)
                                 for column, value in enumerate(values, 1)}})
                if rows:
                    yield prefix + sheet.title, rows, truncated
        finally:
            workbook.close()

    def csv_sheet(payload, label):
        text = payload.decode("utf-8-sig")
        dialect = csv.Sniffer().sniff(text[:8192], delimiters=",;\t")
        reader = csv.reader(io.StringIO(text), dialect)
        headers = next(reader, [])
        if not headers or len(set(headers)) != len(headers) or not all(headers):
            raise ProbeError("CSV headers are empty or repeated; original response preserved.")
        rows = []
        for values in islice(reader, row_limit + 1):
            check_time()
            if len(rows) >= row_limit:
                yield label, rows, True
                return
            if len(values) != len(headers):
                raise ProbeError("CSV row width differs from headers; original response preserved.")
            rows.append(dict(zip(headers, values)))
        if rows:
            yield label, rows, False

    if kind == "xlsx":
        yield from workbook_sheets(body, "")
    elif kind == "csv":
        yield from csv_sheet(body, "csv")
    elif kind == "zip":
        with _archive(body, byte_limit) as archive:
            files = [name for name in archive.namelist() if name.lower().endswith((".xlsx", ".csv")) and not name.startswith("__MACOSX/")]
            for name in files:
                check_time()
                payload = archive.read(name)
                if name.lower().endswith(".xlsx"):
                    yield from workbook_sheets(payload, name + " / ")
                else:
                    yield from csv_sheet(payload, name)
    else:
        raise ProbeError("No tabular export implemented for this format.")

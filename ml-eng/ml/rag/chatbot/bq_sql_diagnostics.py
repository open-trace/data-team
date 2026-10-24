"""Helpers for safe, bounded SQL visibility in diagnostics."""
from __future__ import annotations

import re
from typing import Any

_REDACTED = "<REDACTED>"
MAX_DIAGNOSTIC_SQL_STATEMENTS = 20
MAX_DIAGNOSTIC_SQL_LENGTH = 4_000
_NUMBER_RE = re.compile(
    r"(?:0[xX][0-9A-Fa-f]+|(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)"
)


def redact_sql_literals(sql: str, *, max_length: int = 20_000) -> str:
    """Redact SQL literal values and comments while retaining query structure."""
    text = sql[:max_length]
    out: list[str] = []
    i = 0

    while i < len(text):
        if text.startswith("--", i):
            end = text.find("\n", i + 2)
            if end < 0:
                out.append(f"-- {_REDACTED}")
                break
            out.extend(("-- ", _REDACTED, "\n"))
            i = end + 1
            continue
        if text.startswith("/*", i):
            end = text.find("*/", i + 2)
            if end < 0:
                out.append(f"/* {_REDACTED}")
                break
            out.extend(("/* ", _REDACTED, " */"))
            i = end + 2
            continue

        quote = text[i]
        if quote in ("'", '"'):
            delimiter = quote * 3 if text.startswith(quote * 3, i) else quote
            out.extend((delimiter, _REDACTED, delimiter))
            i += len(delimiter)
            while i < len(text):
                if text[i] == "\\":
                    i += 2
                    continue
                if len(delimiter) == 1 and text.startswith(quote * 2, i):
                    i += 2
                    continue
                if text.startswith(delimiter, i):
                    i += len(delimiter)
                    break
                i += 1
            continue

        if quote.isdigit() or (quote == "." and i + 1 < len(text) and text[i + 1].isdigit()):
            previous = text[i - 1] if i else ""
            match = _NUMBER_RE.match(text, i)
            if (
                match
                and not (previous.isalnum() or previous in "_`.$")
                and not (match.end() < len(text) and (text[match.end()].isalnum() or text[match.end()] in "_$"))
            ):
                out.append(_REDACTED)
                i = match.end()
                continue

        out.append(quote)
        i += 1

    redacted = "".join(out)
    if len(sql) > max_length:
        return f"{redacted} /* SQL truncated for diagnostics */"
    return redacted


def sql_statements_for_diagnostics(value: Any) -> list[str]:
    """Return a bounded list of SQL statements with literal values redacted."""
    if not isinstance(value, list):
        return []
    return [
        redact_sql_literals(sql, max_length=MAX_DIAGNOSTIC_SQL_LENGTH)
        for item in value[:MAX_DIAGNOSTIC_SQL_STATEMENTS]
        if (sql := str(item or "").strip())
    ]


def sql_debug_rows_for_diagnostics(value: Any) -> list[dict[str, Any]]:
    """Copy bounded SQL debug rows and redact SQL or error text."""
    if not isinstance(value, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in value[:MAX_DIAGNOSTIC_SQL_STATEMENTS]:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        for key in ("sql", "nl2sql_raw", "prep_error", "execution_error"):
            text = row.get(key)
            if text is not None:
                row[key] = redact_sql_literals(
                    str(text),
                    max_length=MAX_DIAGNOSTIC_SQL_LENGTH,
                )
        rows.append(row)
    return rows


def sql_attempts_for_diagnostics(value: Any) -> list[dict[str, Any]]:
    """Return bounded query attempts with status and job-ID evidence."""
    if not isinstance(value, list):
        return []
    attempts: list[dict[str, Any]] = []
    for item in value[:MAX_DIAGNOSTIC_SQL_STATEMENTS]:
        if not isinstance(item, dict) or not item.get("sql"):
            continue
        attempts.append(
            {
                "sql": redact_sql_literals(
                    str(item["sql"]),
                    max_length=MAX_DIAGNOSTIC_SQL_LENGTH,
                ),
                "status": str(item.get("status") or "unknown"),
                "job_id": item.get("job_id"),
                "sql_source": item.get("sql_source"),
            }
        )
    return attempts

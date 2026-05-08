"""Airbyte public API client (urllib only — no extra deps).

Endpoints align with OSS Airbyte (see Airbyte docs for your version).
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any


def _base_url() -> str:
    return os.environ.get("AIRBYTE_URL", "http://localhost:8000").rstrip("/")


def _headers() -> dict[str, str]:
    h = {"Content-Type": "application/json"}
    token = os.environ.get("AIRBYTE_CLIENT_TOKEN", "").strip()
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _post_json(path: str, body: dict[str, Any], timeout: int = 120) -> dict[str, Any]:
    url = f"{_base_url()}{path}"
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=_headers(), method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def trigger_connection_sync(connection_id: str) -> dict[str, Any]:
    """POST /api/v1/connections/sync — returns payload including job info."""
    return _post_json("/api/v1/connections/sync", {"connectionId": connection_id})


def extract_job_id(sync_response: dict[str, Any]) -> int | None:
    job = sync_response.get("job") or {}
    jid = job.get("id")
    if jid is None:
        return None
    try:
        return int(jid)
    except (TypeError, ValueError):
        return None


def get_job(job_id: int) -> dict[str, Any]:
    """POST /api/v1/jobs/get"""
    return _post_json("/api/v1/jobs/get", {"id": job_id})


def job_status(job_payload: dict[str, Any]) -> str | None:
    job = job_payload.get("job") or job_payload
    st = job.get("status")
    if st is None:
        return None
    return str(st).lower()


_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "incomplete", "completed"})


def is_terminal_status(status: str | None) -> bool:
    if not status:
        return False
    return status in _TERMINAL


def poll_job(
    job_id: int,
    *,
    poll_interval_sec: float = 10.0,
    max_wait_sec: float = 3600.0,
) -> dict[str, Any]:
    """Poll until terminal status or timeout. Raises RuntimeError on failure/incomplete/timeout."""
    deadline = time.monotonic() + max_wait_sec
    last = None
    while time.monotonic() < deadline:
        last = get_job(job_id)
        st = job_status(last)
        if st == "succeeded" or st == "completed":
            return last
        if st in ("failed", "cancelled", "incomplete"):
            raise RuntimeError(f"Airbyte job {job_id} ended with status={st!r}: {last!r}")
        if is_terminal_status(st):
            raise RuntimeError(f"Airbyte job {job_id} terminal unexpected status={st!r}: {last!r}")
        time.sleep(poll_interval_sec)
    raise TimeoutError(f"Airbyte job {job_id} did not finish within {max_wait_sec}s; last={last!r}")

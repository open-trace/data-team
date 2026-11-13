"""HTTP session with retries on 429/5xx."""

from __future__ import annotations

import time
from typing import Any

import requests

DEFAULT_USER_AGENT = "OpenTrace-ingestion/0.1 (+https://github.com/open-trace/data-team)"
RETRY_STATUSES = {429, 500, 502, 503, 504}


class HttpError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def make_session(user_agent: str = DEFAULT_USER_AGENT) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent, "Accept": "*/*"})
    return session


def get_with_backoff(
    url: str,
    *,
    session: requests.Session | None = None,
    params: dict[str, Any] | None = None,
    timeout: float = 60.0,
    max_retries: int = 3,
    base_delay: float = 1.0,
    sleep_fn=time.sleep,
) -> requests.Response:
    """GET with exponential backoff on 429/5xx. Terminal 4xx raises immediately."""
    sess = session or make_session()
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            resp = sess.get(url, params=params, timeout=timeout)
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_retries:
                raise HttpError(f"Request failed after {max_retries + 1} attempts: {exc}") from exc
            sleep_fn(base_delay * (2**attempt))
            continue

        if resp.status_code in RETRY_STATUSES:
            if attempt >= max_retries:
                raise HttpError(
                    f"GET {url} failed with {resp.status_code} after {max_retries + 1} attempts",
                    status_code=resp.status_code,
                )
            sleep_fn(base_delay * (2**attempt))
            continue

        if 400 <= resp.status_code < 500:
            raise HttpError(
                f"GET {url} failed with terminal status {resp.status_code}: {resp.text[:200]}",
                status_code=resp.status_code,
            )

        if resp.status_code >= 500:
            raise HttpError(
                f"GET {url} failed with status {resp.status_code}",
                status_code=resp.status_code,
            )

        return resp

    raise HttpError(f"GET {url} failed: {last_exc}")

#!/usr/bin/env python3
"""Trigger an Airbyte connection sync via the public API (example stub).

Set AIRBYTE_URL and AIRBYTE_CLIENT_TOKEN (or basic auth) in your environment.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def trigger_sync(connection_id: str) -> dict:
    base = os.environ.get("AIRBYTE_URL", "http://localhost:8000").rstrip("/")
    token = os.environ.get("AIRBYTE_CLIENT_TOKEN", "")
    url = f"{base}/api/v1/connections/sync"
    body = json.dumps({"connectionId": connection_id}).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            **({"Authorization": f"Bearer {token}"} if token else {}),
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode())


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: trigger_sync.py <connection_id>", file=sys.stderr)
        sys.exit(2)
    try:
        out = trigger_sync(sys.argv[1])
        print(json.dumps(out, indent=2))
    except urllib.error.HTTPError as e:
        print(e.read().decode(), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

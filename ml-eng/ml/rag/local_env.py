"""Load ``data/local/.env`` with predictable merge rules for local dev."""
from __future__ import annotations

import os
from pathlib import Path


def load_data_local_dotenv(repo_root: Path) -> None:
    """
    Load ``data/local/.env`` under ``repo_root``.

    - Sets keys that are not already in the environment.
    - If a key exists but is **empty** (e.g. ``export HF_API_TOKEN=`` in the shell), the
      value from ``.env`` is applied so ``.env`` is not silently ignored.
    - If ``RAG_DOTENV_OVERRIDE=1``, always apply ``.env`` over existing values.
    """
    path = repo_root / "data" / "local" / ".env"
    if not path.is_file():
        return
    override = os.environ.get("RAG_DOTENV_OVERRIDE", "").strip().lower() in ("1", "true", "yes")
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if not k:
            continue
        if override:
            os.environ[k] = v
        elif k not in os.environ:
            os.environ[k] = v
        elif not os.environ.get(k, "").strip() and v:
            os.environ[k] = v

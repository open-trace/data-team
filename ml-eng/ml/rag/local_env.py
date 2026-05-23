"""Load ``data/local/.env`` with predictable merge rules for local dev."""
from __future__ import annotations

import os
from pathlib import Path

# Always prefer file values for these when loading data/local/.env (avoids stale shell exports).
_QDRANT_FORCE_KEYS = frozenset({"QDRANT_URL", "QDRANT_API_KEY"})


def _parse_dotenv_line(line: str) -> tuple[str, str] | None:
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        return None
    k, _, v = line.partition("=")
    k = k.strip()
    if k.lower().startswith("export "):
        k = k[7:].strip()
    v = v.strip().strip('"').strip("'")
    if not k:
        return None
    return k, v


def load_data_local_dotenv(repo_root: Path, *, force_keys: frozenset[str] | None = None) -> None:
    """
    Load ``data/local/.env`` under ``repo_root``.

    - Sets keys that are not already in the environment.
    - If a key exists but is **empty** (e.g. ``export HF_API_TOKEN=`` in the shell), the
      value from ``.env`` is applied so ``.env`` is not silently ignored.
    - If ``RAG_DOTENV_OVERRIDE=1``, always apply ``.env`` over existing values.
    - ``force_keys`` (default: QDRANT_URL, QDRANT_API_KEY) always taken from the file when set.
    """
    path = repo_root / "data" / "local" / ".env"
    if not path.is_file():
        return
    override = os.environ.get("RAG_DOTENV_OVERRIDE", "").strip().lower() in ("1", "true", "yes")
    forced = force_keys if force_keys is not None else _QDRANT_FORCE_KEYS
    for line in path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_dotenv_line(line)
        if parsed is None:
            continue
        k, v = parsed
        if override or k in forced:
            os.environ[k] = v
        elif k not in os.environ:
            os.environ[k] = v
        elif not os.environ.get(k, "").strip() and v:
            os.environ[k] = v

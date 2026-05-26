"""Load ``data/local/.env`` and ``config/.env`` with predictable merge rules for local dev."""
from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

# Always prefer file values for these when loading data/local/.env (avoids stale shell exports).
_QDRANT_FORCE_KEYS = frozenset({"QDRANT_URL", "QDRANT_API_KEY"})

_DEFAULT_HF_HOME_REL = Path("data/local/models/chunking/huggingface")
_DEFAULT_GCP_KEY_REL = Path("config/keys/opentrace-bq-key.json")


# #region agent log
_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]  # data-team workspace root


def _agent_debug_log(location: str, message: str, data: dict, hypothesis_id: str, run_id: str = "pre-fix") -> None:
    try:
        payload = {
            "sessionId": "6c8b2f",
            "id": f"log_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}",
            "timestamp": int(time.time() * 1000),
            "location": location,
            "message": message,
            "data": data,
            "runId": run_id,
            "hypothesisId": hypothesis_id,
        }
        with (_WORKSPACE_ROOT / "debug-6c8b2f.log").open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        pass


# #endregion


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
    _merge_dotenv_file(
        repo_root / "data" / "local" / ".env",
        force_keys=force_keys if force_keys is not None else _QDRANT_FORCE_KEYS,
    )


def _merge_dotenv_file(path: Path, *, force_keys: frozenset[str] | None = None) -> None:
    if not path.is_file():
        return
    override = os.environ.get("RAG_DOTENV_OVERRIDE", "").strip().lower() in ("1", "true", "yes")
    forced = force_keys or frozenset()
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


def load_config_dotenv(repo_root: Path) -> None:
    """Load ``config/.env`` under ``repo_root`` (fills keys not set by ``data/local/.env``)."""
    _merge_dotenv_file(repo_root / "config" / ".env")


def _resolve_repo_path(repo_root: Path, raw: str) -> Path | None:
    text = raw.strip().strip('"').strip("'")
    if not text:
        return None
    path = Path(text)
    if path.is_absolute():
        return path if path.exists() else None
    candidate = (repo_root / path).resolve()
    return candidate if candidate.exists() else None


def apply_ml_eng_path_defaults(repo_root: Path) -> None:
    """
    Resolve repo-relative paths and apply sensible defaults for local dev.

    - ``HF_HOME`` → ``data/local/models/chunking/huggingface`` when that hub cache exists
    - ``GOOGLE_APPLICATION_CREDENTIALS`` → resolved relative to ``repo_root``, with fallback
      to ``config/keys/opentrace-bq-key.json``
    """
    hf_raw = os.environ.get("HF_HOME", "").strip()
    if hf_raw:
        resolved = _resolve_repo_path(repo_root, hf_raw)
        if resolved is not None:
            os.environ["HF_HOME"] = str(resolved)
    else:
        default_hf = (repo_root / _DEFAULT_HF_HOME_REL).resolve()
        if (default_hf / "hub").is_dir():
            os.environ["HF_HOME"] = str(default_hf)

    gcp_raw = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if gcp_raw:
        resolved = _resolve_repo_path(repo_root, gcp_raw)
        if resolved is not None and resolved.is_file():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(resolved)
            return
    fallback = (repo_root / _DEFAULT_GCP_KEY_REL).resolve()
    if fallback.is_file():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(fallback)


def apply_lm_studio_defaults() -> None:
    """When ``RAG_LLM_BASE_URL`` is set, apply safe defaults for local LM Studio dev."""
    if not os.environ.get("RAG_LLM_BASE_URL", "").strip():
        return
    for key, value in (
        ("RAG_LLM_RERANK", "off"),
        ("RAG_LLM_TIMEOUT_S", "300"),
        ("RAG_GENERATE_MAX_TOKENS", "1024"),
        ("RAG_GENERATE_TIMEOUT_S", "300"),
        ("RAG_BQ_NL2SQL_PARALLEL", "off"),
        ("RAG_BQ_SKIP_LIVE_SCHEMA", "on"),
        ("RAG_BQ_NL2SQL_TIMEOUT_S", "300"),
    ):
        if not os.environ.get(key, "").strip():
            os.environ[key] = value


def load_rag_dotenv(repo_root: Path) -> None:
    """Load local + config env files, then apply path defaults (HF cache, GCP key)."""
    # #region agent log
    _agent_debug_log(
        "local_env.py:load_rag_dotenv:entry",
        "env load entry",
        {
            "repo_root": str(repo_root),
            "cwd": str(Path.cwd()),
            "data_local_exists": (repo_root / "data" / "local" / ".env").is_file(),
            "config_env_exists": (repo_root / "config" / ".env").is_file(),
        },
        "A",
    )
    # #endregion
    load_data_local_dotenv(repo_root)
    load_config_dotenv(repo_root)
    apply_ml_eng_path_defaults(repo_root)
    apply_lm_studio_defaults()
    # #region agent log
    _agent_debug_log(
        "local_env.py:load_rag_dotenv:after",
        "env load result",
        {
            "qdrant_url_present": bool(os.environ.get("QDRANT_URL", "").strip()),
            "qdrant_api_key_present": bool(os.environ.get("QDRANT_API_KEY", "").strip()),
            "qdrant_url_len": len(os.environ.get("QDRANT_URL", "")),
            "qdrant_api_key_len": len(os.environ.get("QDRANT_API_KEY", "")),
            "rag_dotenv_override": os.environ.get("RAG_DOTENV_OVERRIDE", ""),
        },
        "B",
    )
    # #endregion

"""
Single entry point for connecting to the local database (PostgreSQL or SQLite).
Use this module whenever you read from, write to, or create tables/datasets in the local DB.

Usage:
  From repo root or with data/local/scripts on PYTHONPATH:
    from engine_connector import get_engine, get_config, connection

  In code:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))

    # Or use the context manager:
    with connection() as conn:
        conn.execute(text("CREATE TABLE ..."))

  Config comes from env: LOCAL_DB_URL (PostgreSQL) or LOCAL_DB_PATH (SQLite).
  See data/local/.env.example.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlalchemy.engine import Connection

REPO_ROOT = Path(__file__).resolve().parents[3]


def get_config(
    *,
    local_db_url: str | None = None,
    local_db_path: str | None = None,
) -> dict[str, str]:
    """
    Return config for the local DB. Uses env vars if arguments are not provided.
    Resolves LOCAL_DB_PATH relative to repo root when not absolute.
    """
    url = (local_db_url or os.environ.get("LOCAL_DB_URL", "") or "").strip()
    path = local_db_path or os.environ.get("LOCAL_DB_PATH") or str(REPO_ROOT / "data" / "local" / "local.db")
    path = _resolve_path(path)
    return {"local_db_url": url, "local_db_path": path}


def _resolve_path(path: str) -> str:
    p = Path(path)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return str(p)


def get_engine(
    *,
    local_db_url: str | None = None,
    local_db_path: str | None = None,
    config: dict | None = None,
) -> Engine:
    """
    Return a SQLAlchemy engine for the local database.
    Use this for all read/write and DDL (creating tables, datasets).

    Precedence: config dict (if provided) > keyword args > env vars.
    """
    from sqlalchemy import create_engine

    if config is not None:
        url = (config.get("local_db_url") or "").strip()
        path = config.get("local_db_path") or str(REPO_ROOT / "data" / "local" / "local.db")
        path = _resolve_path(path) if path else ""
    else:
        cfg = get_config(local_db_url=local_db_url, local_db_path=local_db_path)
        url = cfg["local_db_url"]
        path = cfg["local_db_path"]

    if url:
        if not url.startswith("postgresql"):
            url = f"postgresql+psycopg2://{url}" if "://" not in url else url
        if url.startswith("postgresql://") and "+" not in url:
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
        return create_engine(url)

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{path}")


@contextmanager
def connection(
    *,
    local_db_url: str | None = None,
    local_db_path: str | None = None,
    config: dict | None = None,
    commit: bool = True,
):
    """
    Context manager that yields a connection to the local DB.
    Use for reading, writing, or running DDL (CREATE TABLE, etc.).

    If commit=True (default), the connection is used in a transaction that commits on exit.
    """
    engine = get_engine(local_db_url=local_db_url, local_db_path=local_db_path, config=config)
    if commit:
        with engine.begin() as conn:
            yield conn
    else:
        conn = engine.connect()
        try:
            yield conn
        finally:
            conn.close()


def get_connection_url(mask_password: bool = True) -> str:
    """
    Return the connection URL or path used for the local DB (for display or logging).
    If mask_password=True, redacts the password in Postgres URLs.
    """
    cfg = get_config()
    if cfg["local_db_url"]:
        u = cfg["local_db_url"]
        if mask_password and "://" in u and "@" in u:
            try:
                before_at = u.split("@", 1)[0]
                after_at = u.split("@", 1)[1]
                if ":" in before_at:
                    user_part = before_at.split(":", 1)[0] + ":****"
                    return user_part + "@" + after_at
            except IndexError:
                pass
        return u
    return cfg["local_db_path"]

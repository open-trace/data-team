"""Structured logging with run_id on every line."""

from __future__ import annotations

import logging
import sys
from typing import Any


class RunIdFilter(logging.Filter):
    def __init__(self, run_id: str = "-") -> None:
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id  # type: ignore[attr-defined]
        return True


def get_logger(name: str = "ingestion", run_id: str = "-") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s run_id=%(run_id)s %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    # Keep a single filter; replace run_id when updated.
    logger.filters.clear()
    logger.addFilter(RunIdFilter(run_id))
    return logger


def bind_run_id(logger: logging.Logger, run_id: str) -> None:
    for f in list(logger.filters):
        if isinstance(f, RunIdFilter):
            f.run_id = run_id
            return
    logger.addFilter(RunIdFilter(run_id))


def log_kv(logger: logging.Logger, msg: str, **kwargs: Any) -> None:
    extras = " ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.info("%s %s", msg, extras) if extras else logger.info(msg)

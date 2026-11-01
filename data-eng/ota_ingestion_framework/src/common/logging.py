# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Shared logger setup with source/run context in each message."""

from __future__ import annotations

import logging as _logging


def get_logger(name: str) -> _logging.Logger:
    logger = _logging.getLogger(name)
    if not logger.handlers:
        handler = _logging.StreamHandler()
        handler.setFormatter(
            _logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(_logging.INFO)
    return logger

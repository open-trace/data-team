#!/usr/bin/env python3
"""Deprecated alias: sync raw_dev layer. Prefer sync_all_tables.py --dataset raw_dev."""

from __future__ import annotations

import sys

from sync_all_tables import main as sync_main


def main() -> None:
    sys.argv = [sys.argv[0], "--dataset", "raw_dev", *sys.argv[1:]]
    sync_main()


if __name__ == "__main__":
    main()

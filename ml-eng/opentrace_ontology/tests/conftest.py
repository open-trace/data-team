"""Build ontology.sqlite once per session if missing (offline, no network)."""
from __future__ import annotations

import os
import subprocess
import sys

import pytest

PKG = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BUILD = os.path.join(PKG, "ontology_build")
DATA = os.path.join(PKG, "data")
DB = os.path.join(DATA, "ontology.sqlite")
ML_ENG = os.path.abspath(os.path.join(PKG, ".."))
MVI = os.path.join(ML_ENG, "ml", "rag", "value_index", "mart_value_index.json")


@pytest.fixture(scope="session", autouse=True)
def _ensure_ontology_sqlite() -> None:
    if os.path.exists(DB) and os.path.getsize(DB) > 0:
        return
    env = os.environ.copy()
    prev = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = ML_ENG + (os.pathsep + prev if prev else "")
    cmds = [
        [sys.executable, os.path.join(BUILD, "build_sqlite.py")],
        [sys.executable, os.path.join(BUILD, "check_sqlite_matches_csv.py")],
        [sys.executable, os.path.join(BUILD, "build_value_map.py"), "--mvi", MVI],
        [sys.executable, os.path.join(BUILD, "check_value_index_coverage.py"), "--mvi", MVI],
    ]
    for cmd in cmds:
        r = subprocess.run(cmd, cwd=ML_ENG, env=env, capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(
                f"ontology build failed: {' '.join(cmd)}\nstdout={r.stdout}\nstderr={r.stderr}"
            )

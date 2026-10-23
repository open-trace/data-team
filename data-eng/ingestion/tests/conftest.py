"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.delenv("INGEST_ALLOW_PROD", raising=False)
    monkeypatch.setenv("CI", "")
    monkeypatch.setenv("INGEST_TARGET_DATASET", "landing_dev")
    monkeypatch.setenv("GCP_PROJECT", "opentrace-prod-5ga4")
    monkeypatch.setenv("BQ_LOCATION", "europe-west3")
    monkeypatch.setenv("INGEST_OFFLINE_FIXTURES", "1")


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture
def package_root() -> Path:
    return PACKAGE_ROOT

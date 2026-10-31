"""Profile country cleaning + query geography precedence (§6)."""
from __future__ import annotations

import opentrace_ontology
import pytest

from ml.rag.chatbot.geo_policy import (
    apply_profile_geography_default,
    sanitize_profile_country,
)
from ml.rag.chatbot.query_decomposer import resolve_retrieval_geographies


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("NG", "NGA"),
        ("Nigeria", "NGA"),
        ("Nigéria", "NGA"),
        ("NGA", "NGA"),
    ],
)
def test_african_profile_forms_to_iso3(raw: str, expected: str) -> None:
    assert sanitize_profile_country(raw) == expected


@pytest.mark.parametrize("raw", ["Albania", "US", "Unknown", "", None])
def test_non_african_or_unknown_profile_dropped(raw: str | None) -> None:
    assert sanitize_profile_country(raw) is None


def test_accented_country_names_use_fallback_when_ontology_has_no_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(opentrace_ontology, "clean_profile_country", lambda _value: None)

    assert sanitize_profile_country("Nigéria") == "NGA"
    assert sanitize_profile_country("Côte d’Ivoire") == "CIV"


def test_query_geography_wins_over_profile_default() -> None:
    dec = apply_profile_geography_default(
        {"geography": ["Ghana"], "geography_source": "query"},
        "NG",
    )
    assert dec["geography"] == ["Ghana"]
    assert dec.get("geography_source") == "query"
    geos = resolve_retrieval_geographies(geo_override="NGA", geography=dec["geography"])
    assert geos == ["Ghana"]


def test_soft_profile_default_when_query_has_no_country() -> None:
    dec = apply_profile_geography_default({"geography": []}, "NG")
    assert dec["geography"] == ["NGA"]
    assert dec["geography_source"] == "profile"

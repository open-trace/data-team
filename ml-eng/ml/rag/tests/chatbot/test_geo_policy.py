"""Unit tests for Farmers plan_type profile country retrieval policy."""

from __future__ import annotations

from ml.rag.chatbot.geo_policy import (
    FARMER_PLAN_TYPE,
    effective_geo_override,
    profile_country_for_retrieval,
)


def test_farmers_plan_profile_country_is_not_a_hard_override() -> None:
    profile = {"country": "Nigeria", "plan_type": "Farmers", "category": "Farmers"}
    assert profile_country_for_retrieval(FARMER_PLAN_TYPE, profile) == "NGA"
    assert effective_geo_override(FARMER_PLAN_TYPE, profile) == ""


def test_farmers_hard_override_only_when_policy_on(monkeypatch) -> None:
    monkeypatch.setenv("ADZA_SINGLE_COUNTRY_POLICY", "true")
    profile = {"country": "Nigeria", "plan_type": "Farmers", "category": "Farmers"}
    assert effective_geo_override(FARMER_PLAN_TYPE, profile) == "NGA"
    assert effective_geo_override(FARMER_PLAN_TYPE, {"country": "Albania"}) == ""


def test_non_farmer_plan_profile_country_ignored() -> None:
    profile = {"country": "Nigeria", "plan_type": "Government", "category": "Government"}
    assert profile_country_for_retrieval("Government", profile) == ""
    assert profile_country_for_retrieval("Free", profile) == ""


def test_missing_profile_returns_empty() -> None:
    assert profile_country_for_retrieval(FARMER_PLAN_TYPE, None) == ""
    assert profile_country_for_retrieval(FARMER_PLAN_TYPE, {}) == ""
    assert profile_country_for_retrieval(FARMER_PLAN_TYPE, {"country": ""}) == ""


def test_region_token_in_profile_filtered() -> None:
    profile = {"country": "Africa", "plan_type": "Farmers", "category": "Farmers"}
    assert profile_country_for_retrieval(FARMER_PLAN_TYPE, profile) == ""
    profile2 = {"country": "West Africa", "plan_type": "Farmers", "category": "Farmers"}
    assert profile_country_for_retrieval(FARMER_PLAN_TYPE, profile2) == ""

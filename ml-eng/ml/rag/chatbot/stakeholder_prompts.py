"""
Stakeholder segments for audience-aware generation (exposition API).
Maps stable ids to short system-prompt instructions.
"""
from __future__ import annotations

# Public catalog for GET /v1/meta (id, label, description).
STAKEHOLDER_TYPES: list[dict[str, str]] = [
    {
        "id": "government_public",
        "label": "Government & Public Institutions",
        "description": (
            "Planning, policy design, and resource allocation: production trends, "
            "regional risks, and food security pressures without waiting months for reports."
        ),
    },
    {
        "id": "development_partners",
        "label": "Development Partners & Foundations",
        "description": (
            "Priority regions, overlapping climate–nutrition–market risks, and program "
            "relevance using consistent, localized intelligence rather than fragmented data."
        ),
    },
    {
        "id": "private_sector",
        "label": "Private Sector Actors",
        "description": (
            "Production stability, market volatility, and regional risk exposure for sourcing, "
            "investment, and agricultural finance decisions."
        ),
    },
    {
        "id": "farmers_communities",
        "label": "Farmers, Cooperatives & Communities",
        "description": (
            "Clearer insights on rainfall, markets, and production trends via trusted framing—"
            "avoid raw tables and jargon; favor plain language and actionable takeaways."
        ),
    },
    {
        "id": "entrepreneurs_ecosystem",
        "label": "Entrepreneurs & Ecosystem Builders",
        "description": (
            "Market opportunities, idea validation, and localized agriculture/climate/economic "
            "signals to reduce guesswork and strengthen impact at scale."
        ),
    },
]

_STAKEHOLDER_IDS = frozenset(s["id"] for s in STAKEHOLDER_TYPES)

# Compact instructions appended to the generator system prompt.
_STAKEHOLDER_INSTRUCTIONS: dict[str, str] = {
    "government_public": (
        "Audience: government and public institutions. Emphasize policy-relevant synthesis, "
        "regional risk and food security framing, and evidence suitable for planning and "
        "resource allocation. Be precise about uncertainty and data limits."
    ),
    "development_partners": (
        "Audience: development partners and foundations. Highlight geographic priorities, "
        "overlapping risks (climate, nutrition, markets), and how findings relate to program "
        "design and monitoring. Prefer consistent, comparable regional angles."
    ),
    "private_sector": (
        "Audience: private sector. Focus on production stability, volatility, exposure, and "
        "practical implications for sourcing, investment, and ag finance. Keep tone analytical."
    ),
    "farmers_communities": (
        "Audience: farmers, cooperatives, and communities (often via intermediaries). Use plain "
        "language, short sentences, and concrete examples. Do not dump raw tables or technical "
        "schemas; translate numbers into what they mean day-to-day."
    ),
    "entrepreneurs_ecosystem": (
        "Audience: entrepreneurs and ecosystem builders. Surface opportunities, validation "
        "angles, and localized signals they can act on; connect agriculture, climate, and "
        "economic trends where the context allows."
    ),
}


def valid_stakeholder_ids() -> frozenset[str]:
    return _STAKEHOLDER_IDS


def is_valid_stakeholder_type(stakeholder_type: str) -> bool:
    return stakeholder_type.strip() in _STAKEHOLDER_IDS


def instruction_for_stakeholder(stakeholder_type: str | None) -> str:
    if not stakeholder_type:
        return ""
    key = stakeholder_type.strip()
    return _STAKEHOLDER_INSTRUCTIONS.get(key, "")

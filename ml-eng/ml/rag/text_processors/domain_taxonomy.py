"""
Shared agrifood domain taxonomy for RAG payloads (news, PDFs, BQ descriptions, OTA insights).

Use ``infer_domains`` on chunk or document text; emit ``domains`` as a '; '-joined label string.
"""

from __future__ import annotations

import re
from typing import Final

# Canonical taxonomy (aligned with web-mined news corpus labels).
DOMAIN_KEYWORDS: dict[str, tuple[str, ...]] = {
    "agriculture": (
        "agriculture",
        "agricultural",
        "agribusiness",
        "farming",
        "farm",
        "farmer",
        "crop",
        "livestock",
        "smallholder",
        "agronomy",
        "food security",
    ),
    "Agricultural Economics": (
        "farm income",
        "agricultural income",
        "food price",
        "commodity price",
        "farm gate",
        "subsidy",
        "inflation",
        "gdp",
        "rural economy",
    ),
    "Agricultural International Trade (Exports & Imports)": (
        "export",
        "exports",
        "import",
        "imports",
        "cross-border",
        "customs",
        "tariff",
        "trade agreement",
        "phytosanitary",
    ),
    "Agricultural Environmental & Climate": (
        "climate",
        "rainfall",
        "drought",
        "temperature",
        "extreme weather",
        "flood",
        "heatwave",
        "emissions",
    ),
    "Land Use & Soil Health": (
        "land use",
        "soil",
        "erosion",
        "fertility",
        "degradation",
    ),
    "Agricultural Investment Readiness & Enterprise": (
        "investment",
        "enterprise",
        "entrepreneur",
        "finance",
        "credit",
        "loan",
        "bank",
        "funding",
    ),
    "Agricultural Technology & Innovation": (
        "technology",
        "innovation",
        "digital",
        "ai",
        "satellite",
        "remote sensing",
        "irrigation technology",
    ),
    "Agricultural Market Access & Infrastructure": (
        "market access",
        "logistics",
        "infrastructure",
        "transport",
        "storage",
        "post-harvest",
        "warehouse",
        "cold chain",
    ),
    "Agricultural Production & Yield": (
        "yield",
        "production",
        "productivity",
        "harvest",
        "acreage",
    ),
    "Agricultural Policy & Institutional": (
        "policy",
        "regulation",
        "ministry",
        "institution",
        "governance",
        "subsidy",
    ),
    "Agricultural Gender, Youth & Inclusion": (
        "gender",
        "women",
        "youth",
        "inclusion",
        "smallholder",
    ),
    "Agricultural Nutrition & Food Security": (
        "nutrition",
        "food security",
        "malnutrition",
        "hunger",
        "stunting",
        "food insecure",
    ),
    "Agricultural Food Systems & Value Chain": (
        "value chain",
        "food system",
        "processing",
        "distribution",
        "retail",
        "supply chain",
    ),
    "Agricultural Humanitarian & Agricultural Emergency": (
        "humanitarian",
        "emergency",
        "crisis",
        "famine",
        "displacement",
        "conflict",
    ),
}

DEFAULT_MAX_DOMAINS_NEWS: Final[int] = 4
DEFAULT_MAX_DOMAINS_LONG_DOC: Final[int] = 6


def _keyword_hits(text_lower: str, keyword: str) -> int:
    if " " in keyword:
        return len(re.findall(re.escape(keyword), text_lower))
    return len(re.findall(rf"\b{re.escape(keyword)}\b", text_lower))


def infer_domains(text: str, *, max_domains: int = DEFAULT_MAX_DOMAINS_NEWS) -> list[str]:
    """
    Score domains by keyword frequency; return top labels (stable agrifood taxonomy).
    """
    lowered = text.lower()
    scores: list[tuple[str, int]] = []
    for domain, keywords in DOMAIN_KEYWORDS.items():
        total = sum(_keyword_hits(lowered, k) for k in keywords)
        scores.append((domain, total))
    scores.sort(key=lambda x: x[1], reverse=True)
    picked = [d for d, s in scores if s > 0][: max(1, max_domains)]
    return picked or ["agriculture"]


def primary_domain_label(domains: list[str]) -> str:
    return domains[0] if domains else "agriculture"


COUNTRIES: Final[tuple[str, ...]] = (
    "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi", "Cameroon",
    "Cape Verde", "Central African Republic", "Chad", "Comoros", "Congo", "Djibouti",
    "Egypt", "Equatorial Guinea", "Eritrea", "Eswatini", "Ethiopia", "Gabon", "Gambia",
    "Ghana", "Guinea", "Guinea-Bissau", "Ivory Coast", "Cote d'Ivoire", "Kenya", "Lesotho",
    "Liberia", "Libya", "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius",
    "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria", "Rwanda", "Sao Tome and Principe",
    "Senegal", "Seychelles", "Sierra Leone", "Somalia", "South Africa", "South Sudan",
    "Sudan", "Tanzania", "Togo", "Tunisia", "Uganda", "Zambia", "Zimbabwe", "DRC",
    "Democratic Republic of the Congo",
)

COUNTRIES_BY_LENGTH: Final[tuple[str, ...]] = tuple(sorted(COUNTRIES, key=len, reverse=True))
MAX_PLACES: Final[int] = 12
MIN_COUNTRY_MENTIONS: Final[int] = 2


def _main_text_for_inference(full_text: str) -> str:
    for pat in (
        r"\n\s*References\s*\n",
        r"\n\s*REFERENCES\s*\n",
        r"\n\s*Bibliography\s*\n",
        r"\n\s*BIBLIOGRAPHY\s*\n",
        r"\n\s*Works Cited\s*\n",
    ):
        m = re.search(pat, full_text, flags=re.IGNORECASE)
        if m:
            return full_text[: m.start()]
    return full_text


def infer_info_type(full_text: str, source_file: str) -> str:
    body = _main_text_for_inference(full_text)
    lowered = body.lower()
    file_lower = source_file.lower()
    if (
        re.search(r"\bdoi:\s*10\.", body, re.IGNORECASE)
        or re.search(r"\bjournal of\b", lowered)
        or "1-s2.0" in file_lower
        or re.search(r"\babstract\b", lowered[:8000])
    ):
        return "academic_article"
    gov_markers = (
        "national bureau of statistics",
        "official gazette",
        "government of",
        "ministry of",
        "policy brief",
    )
    if sum(1 for m in gov_markers if m in lowered) >= 2:
        return "government_report"
    return "academic_article"


def infer_places_of_focus(full_text: str) -> list[str]:
    body = _main_text_for_inference(full_text)
    counts: dict[str, int] = {}
    for country in COUNTRIES_BY_LENGTH:
        n = len(re.findall(rf"\b{re.escape(country)}\b", body, flags=re.IGNORECASE))
        if n <= 0:
            continue
        canon = "Democratic Republic of the Congo" if country in {"DRC", "Congo"} else country
        counts[canon] = counts.get(canon, 0) + n
    ranked = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    min_m = MIN_COUNTRY_MENTIONS if len(ranked) > 3 else 1
    out = [name for name, c in ranked if c >= min_m][:MAX_PLACES]
    if not out and ranked:
        out = [name for name, _ in ranked[:5]]
    return out

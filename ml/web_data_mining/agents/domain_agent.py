from __future__ import annotations

import re
from typing import Iterable

# At least one hit required before any domain label is assigned — blocks pure politics / macro
# stories that only matched generic words like "government", "investment", or "production".
AGRICULTURE_CONTEXT_TERMS: tuple[str, ...] = (
    "agriculture",
    "agricultural",
    "agribusiness",
    "agrifood",
    "agritech",
    "agrotech",
    "agro-",
    "farmer",
    "farmers",
    "farming",
    "farm",
    "smallholder",
    "livestock",
    "cattle",
    "poultry",
    "pastoral",
    "ranch",
    "crop",
    "crops",
    "harvest",
    "planting",
    "irrigation",
    "fertilizer",
    "fertiliser",
    "pesticide",
    "seed",
    "seeds",
    "soil",
    "hectare",
    "hectares",
    "food security",
    "malnutrition",
    "stunting",
    "hunger crisis",
    "famine",
    "cassava",
    "maize",
    "rice",
    "wheat",
    "cocoa",
    "palm oil",
    "yam",
    "millet",
    "sorghum",
    "aquaculture",
    "fisheries",
    "fishery",
    "extension service",
    "extension officer",
    "agric extension",
    "ministry of agriculture",
    "minister of agriculture",
    "agrarian",
    "farmland",
    "arable",
    "post-harvest",
    "value chain",
    "commodity",
    "farm gate",
    "grain",
    "silos",
    "greenhouse",
    "dairy",
    "milk production",
    "meat processing",
    "rural road",
    "rural market",
    "farm to market",
)

# Domains below are agrifood-scoped. Prefer multi-word / sector-specific phrases over
# bare macro or civic terms (e.g. avoid standalone "government", "investment", "gdp").
DOMAIN_KEYWORDS: dict[str, tuple[str, ...]] = {
    "agriculture": (
        "agriculture",
        "agricultural",
        "farming",
        "farmers",
        "farmer",
        "smallholder",
        "crop production",
        "livestock",
        "on-farm",
        "agronomy",
        "extension service",
        "agric extension",
        # Staple / export crops (also strengthen routing when headline is crop-centric)
        "cassava",
        "maize",
        "rice",
        "wheat",
        "cocoa",
        "palm oil",
        "yam",
        "millet",
        "sorghum",
        "tomato",
        "onion",
        "potato",
        "banana",
        "plantain",
    ),
    "economics": (
        "farm income",
        "agricultural income",
        "food price",
        "food inflation",
        "commodity price",
        "farm gate price",
        "agricultural subsidy",
        "farm subsidy",
        "rural economy",
        "agribusiness revenue",
        "agricultural gdp",
        "farm profitability",
    ),
    "International Trade (Exports & Imports)": (
        "agricultural export",
        "agricultural import",
        "food export",
        "food import",
        "cash crop export",
        "commodity export",
        "bulk grain",
        "cross-border trade",
        "phytosanitary",
        "sanitary and phytosanitary",
        "export ban",
        "import ban",
        "trade barrier",
        "tariff",
        "re-export",
    ),
    "Environmental & Climate": (
        "climate smart agriculture",
        "climate adaptation",
        "drought",
        "rainfall",
        "crop weather",
        "flood damage",
        "agricultural emissions",
        "carbon farming",
        "deforestation",
        "water stress",
        "irrigation water",
    ),
    "Land Use & Soil Health": (
        "soil health",
        "soil fertility",
        "soil erosion",
        "land degradation",
        "farmland",
        "arable land",
        "irrigation scheme",
        "land tenure",
        "rangeland",
    ),
    "Investment Readiness & Enterprise": (
        "agricultural investment",
        "farm investment",
        "agribusiness finance",
        "farm credit",
        "agricultural lending",
        "smallholder finance",
        "rural finance",
        "value chain finance",
        "warehouse receipt",
        "agri-enterprise",
    ),
    "Technology & Innovation": (
        "precision agriculture",
        "agricultural technology",
        "agritech",
        "digital agriculture",
        "farm management software",
        "drone spraying",
        "satellite monitoring",
        "smart irrigation",
        "mechanization",
        "tractor",
    ),
    "Market Access & Infrastructure": (
        "farm to market",
        "market access",
        "farmers market",
        "rural market",
        "cold chain",
        "grain storage",
        "post-harvest loss",
        "rural road",
        "agricultural logistics",
        "aggregation center",
    ),
    "Production & Yield": (
        "crop yield",
        "farm output",
        "harvest season",
        "agricultural productivity",
        "tonnes per hectare",
        "crop failure",
        "livestock productivity",
        "milk yield",
        "egg production",
    ),
    "Policy & Institutional": (
        "agricultural policy",
        "farm policy",
        "agriculture ministry",
        "minister of agriculture",
        "extension policy",
        "land reform",
        "agricultural regulation",
        "rural development policy",
        "farm bill",
        "input subsidy program",
    ),
    "Gender, Youth & Inclusion": (
        "women farmers",
        "female farmer",
        "rural women",
        "youth in agriculture",
        "young farmers",
        "gender gap agriculture",
        "smallholder women",
    ),
    "Nutrition & Food Security": (
        "food security",
        "malnutrition",
        "stunting",
        "diet diversity",
        "food availability",
        "school feeding",
        "fortified food",
        "micronutrient",
    ),
    "Food Systems & Value Chain": (
        "food system",
        "agrifood value chain",
        "food processing",
        "farm to fork",
        "supply chain food",
        "aggregation",
        "food loss",
    ),
    "Humanitarian & Agricultural Emergency": (
        "food aid",
        "crop failure",
        "famine",
        "humanitarian food",
        "agricultural emergency",
        "livestock disease outbreak",
        "displacement food",
        "conflict food security",
    ),
}


def _count_keyword_hits(text_lower: str, keywords: Iterable[str]) -> int:
    n = 0
    for kw in keywords:
        if " " in kw or "-" in kw:
            n += text_lower.count(kw.lower())
        else:
            n += len(re.findall(rf"\b{re.escape(kw.lower())}\b", text_lower))
    return n


def agricultural_context_hits(text: str) -> int:
    """How many agriculture-context signals appear (used as a minimum gate)."""
    return _count_keyword_hits(text.lower(), AGRICULTURE_CONTEXT_TERMS)


def agricultural_context_signals(text: str) -> list[str]:
    lowered = text.lower()
    out: list[str] = []
    for term in AGRICULTURE_CONTEXT_TERMS:
        if " " in term or "-" in term:
            if term.lower() in lowered:
                out.append(term)
        elif re.search(rf"\b{re.escape(term.lower())}\b", lowered):
            out.append(term)
    return out


class DomainAgentRegistry:
    """
    One logical 'agent' per domain: keyword-based scoring over short text (RSS) or full article.
    Requires at least one agrifood-context signal before assigning any domain.
    """

    def __init__(self, active_domains: list[str] | None = None) -> None:
        self._domains = active_domains or list(DOMAIN_KEYWORDS.keys())

    def scores(self, text: str) -> dict[str, int]:
        lowered = text.lower()
        out: dict[str, int] = {}
        for domain in self._domains:
            kws = DOMAIN_KEYWORDS.get(domain, ())
            if not kws:
                continue
            out[domain] = _count_keyword_hits(lowered, kws)
        return out

    def best_domain(self, text: str) -> tuple[str, int]:
        if agricultural_context_hits(text) < 1:
            return "", 0
        scores = self.scores(text)
        if not scores or max(scores.values(), default=0) == 0:
            return "", 0
        return max(scores.items(), key=lambda x: x[1])

    def ranked_labels(self, text: str) -> list[str]:
        if agricultural_context_hits(text) < 1:
            return []
        sc = self.scores(text)
        return [k for (k, v) in sorted(sc.items(), key=lambda x: x[1], reverse=True) if v > 0]

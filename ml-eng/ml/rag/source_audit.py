"""Audit priority source coverage for geography and crop ingestion gaps."""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PrioritySource:
    name: str
    category: str
    geographies: tuple[str, ...] = ()
    crops: tuple[str, ...] = ()
    description: str = ""


def _normalize_items(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, (list, tuple, set)):
        out: list[str] = []
        for item in values:
            text = str(item).strip()
            if text:
                out.append(text)
        return out
    text = str(values).strip()
    return [text] if text else []


DEFAULT_PRIORITY_SOURCES: list[dict[str, Any]] = [
    {
        "name": "market_prices",
        "category": "market",
        "geographies": ["Kenya", "Nigeria", "Ethiopia", "Ghana"],
        "crops": ["maize", "sorghum", "rice", "wheat"],
        "description": "Regional staple crop and commodity price monitoring",
    },
    {
        "name": "weather",
        "category": "weather",
        "geographies": ["Kenya", "Uganda", "Rwanda", "Tanzania"],
        "crops": ["maize", "beans", "coffee", "tea"],
        "description": "Rainfall and climate anomaly signals for crop planning",
    },
    {
        "name": "satellite_vegetation",
        "category": "satellite",
        "geographies": ["Ethiopia", "Sudan", "Mali", "Senegal"],
        "crops": ["millet", "sorghum", "rice", "cassava"],
        "description": "NDVI and vegetation condition monitoring",
    },
    {
        "name": "food_security",
        "category": "food_security",
        "geographies": ["Somalia", "South Sudan", "Niger", "Burkina Faso"],
        "crops": ["millet", "sorghum", "maize", "cowpeas"],
        "description": "Food insecurity and household resilience signals",
    },
]


def audit_priority_sources(
    *,
    priority_sources: list[PrioritySource | dict[str, Any]],
    current_sources: list[str] | list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return a compact report of ingestion gaps by geography and crop type.

    A source that is already present is assumed to have baseline coverage for its
    first geography and first crop; the remaining values are flagged as expansion gaps.
    """

    normalized_current = {
        str(item).strip().lower()
        for item in (current_sources or [])
        if isinstance(item, str) and str(item).strip()
    }
    if current_sources:
        for item in current_sources:
            if isinstance(item, dict):
                name = str(item.get("name") or "").strip().lower()
                if name:
                    normalized_current.add(name)

    report: list[dict[str, Any]] = []
    for source in priority_sources:
        if isinstance(source, PrioritySource):
            name = source.name
            category = source.category
            geographies = list(source.geographies)
            crops = list(source.crops)
            description = source.description
        else:
            name = str(source.get("name") or "").strip()
            category = str(source.get("category") or "").strip()
            geographies = _normalize_items(source.get("geographies") or [])
            crops = _normalize_items(source.get("crops") or [])
            description = str(source.get("description") or "")

        is_current = name.strip().lower() in normalized_current
        covered_geographies = [geographies[0]] if geographies and is_current else []
        covered_crops = [crops[0]] if crops and is_current else []

        gaps = {
            "geographies": [g for g in geographies if g not in covered_geographies],
            "crops": [c for c in crops if c not in covered_crops],
        }
        if not is_current:
            gaps = {"geographies": geographies, "crops": crops}

        report.append(
            {
                "name": name,
                "category": category,
                "description": description,
                "is_current": is_current,
                "gaps": gaps,
            }
        )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit priority source coverage gaps by geography and crop type")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a text summary")
    parser.add_argument(
        "--current-source",
        action="append",
        default=[],
        help="A source already ingested; repeat flag for multiple values",
    )
    args = parser.parse_args()

    report = audit_priority_sources(
        priority_sources=DEFAULT_PRIORITY_SOURCES,
        current_sources=args.current_source,
    )
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0

    for item in report:
        gaps = item["gaps"]
        print(f"- {item['name']} ({item['category']}): current={item['is_current']}")
        if gaps["geographies"]:
            print(f"  geography gaps: {', '.join(gaps['geographies'])}")
        if gaps["crops"]:
            print(f"  crop gaps: {', '.join(gaps['crops'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

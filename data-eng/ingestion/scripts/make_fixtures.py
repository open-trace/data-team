"""Generate offline test fixtures (run once during setup)."""

from __future__ import annotations

import json
from pathlib import Path

from openpyxl import Workbook

FIX = Path(__file__).resolve().parents[1] / "tests" / "fixtures"


def main() -> None:
    FIX.mkdir(parents=True, exist_ok=True)

    header = (
        "date,market_id,market,commodity_id,commodity,pricetype,unit,"
        "price,currency,usdprice,latitude,longitude"
    )
    hxl = (
        "#date,#meta+id,#loc+market,#meta+id,#item+name,#meta+type,#meta+unit,"
        "#value,#meta+currency,#value+usd,#geo+lat,#geo+lon"
    )
    rows = []
    for i in range(50):
        rows.append(
            f"2024-01-{(i % 28) + 1:02d},{100 + i},Market{i % 5},{200 + i % 3},"
            f"Maize,Retail,KG,{100 + i}.5,NGN,{0.1 + i * 0.01},9.0,7.0"
        )
    (FIX / "hdx_wfp_nga_sample.csv").write_text(
        header + "\n" + hxl + "\n" + "\n".join(rows) + "\n", encoding="utf-8"
    )

    wb = Workbook()
    ws = wb.active
    if ws is None:
        ws = wb.create_sheet("Monthly Prices")
    else:
        ws.title = "Monthly Prices"
    ws.append(["", "Crude oil, average", "Cocoa"])
    ws.append(["", "$/bbl", "$/kg"])
    ws.append(["2024M01", 80.5, 2.1])
    ws.append(["2024M02", 81.0, 2.2])
    ws.append(["2024M03", 79.5, 2.0])
    wb.save(FIX / "pink_sheet_sample.xlsx")

    p1 = {
        "found": 5,
        "data": [
            {
                "country": "Nigeria",
                "market": "Lagos",
                "date": "2024-01-01",
                "commodity": "Rice",
                "price": 1.2,
            },
            {
                "country": "Nigeria",
                "market": "Kano",
                "date": "2024-01-01",
                "commodity": "Maize",
                "price": 0.8,
            },
            {
                "country": "Kenya",
                "market": "Nairobi",
                "date": "2024-01-02",
                "commodity": "Beans",
                "price": 1.1,
            },
        ],
    }
    p2 = {
        "found": 5,
        "data": [
            {
                "country": "Ghana",
                "market": "Accra",
                "date": "2024-01-03",
                "commodity": "Rice",
                "price": 1.3,
            },
            {
                "country": "Ethiopia",
                "market": "Addis",
                "date": "2024-01-04",
                "commodity": "Wheat",
                "price": 0.9,
            },
        ],
    }
    (FIX / "rtfp_page1.json").write_text(json.dumps(p1), encoding="utf-8")
    (FIX / "rtfp_page2.json").write_text(json.dumps(p2), encoding="utf-8")

    u1 = {
        "data": [
            {"dataset_id": "DS1", "record": {"id": "r1", "title": "A"}},
            {"dataset_id": "DS1", "record": {"id": "r2", "title": "B"}},
        ]
    }
    u2 = {
        "data": [
            {"dataset_id": "DS2", "record": {"id": "r3", "title": "C"}},
        ]
    }
    (FIX / "un_page1.json").write_text(json.dumps(u1), encoding="utf-8")
    (FIX / "un_page2.json").write_text(json.dumps(u2), encoding="utf-8")
    print("wrote", list(FIX.iterdir()))


if __name__ == "__main__":
    main()

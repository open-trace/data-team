# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Fixed-column CSV export from collected JSONL price records."""

import csv
import json
from pathlib import Path

PRICE_COLUMNS = [
    "observation_id", "source_id", "run_id", "observation_date", "retrieved_at_utc",
    "market", "commodity_source", "package_id_source", "grade_source", "container_source",
    "province_source", "count_source", "package_mass_source", "mass_unit", "currency",
    "price_basis", "low_price", "average_price", "high_price", "quantity_sold", "stock",
    "opening_balance", "sales_value", "total_mass_source", "rand_per_kg_source",
    "voided_source", "quality_status", "quality_issues", "response_file",
]


def price_csv_row(record):
    """Flatten one record; None becomes an empty CSV cell, never zero."""
    row = {column: record.get(column) for column in PRICE_COLUMNS}
    row["observation_date"] = record["observed_at"]
    row["quality_issues"] = json.dumps(record.get("quality_issues", []), ensure_ascii=False)
    return row


def export_prices_csv(jsonl_path, csv_path):
    """Stream the saved JSONL into a CSV; publish it only after export completes."""
    jsonl_path, csv_path = Path(jsonl_path), Path(csv_path)
    temporary = csv_path.with_suffix(".csv.tmp")
    count = 0
    try:
        with jsonl_path.open(encoding="utf-8") as records, temporary.open("x", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=PRICE_COLUMNS, lineterminator="\n")
            writer.writeheader()
            for line in records:
                if not line.strip():
                    continue
                writer.writerow(price_csv_row(json.loads(line)))
                count += 1
        if csv_path.exists():
            raise FileExistsError(csv_path)
        temporary.rename(csv_path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return count

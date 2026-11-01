# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Write source-shaped records without applying price business mappings."""

import csv
import json


def write_source_records(records, jsonl_path, csv_path):
    """Keep every JSON object and export the union of observed columns to CSV.

    Missing and null values become blank CSV cells. JSONL preserves which keys
    were actually present, including explicit nulls and nested source values.
    """
    columns = list(dict.fromkeys(key for record in records for key in record))
    jsonl_temp = jsonl_path.with_suffix(".jsonl.tmp")
    csv_temp = csv_path.with_suffix(".csv.tmp")
    if jsonl_path.exists() or csv_path.exists() or jsonl_temp.exists() or csv_temp.exists():
        raise FileExistsError("Record output already exists; refusing to overwrite it.")
    try:
        with jsonl_temp.open("x", encoding="utf-8") as jsonl, csv_temp.open("x", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
            if columns:
                writer.writeheader()
            for record in records:
                jsonl.write(json.dumps(record, ensure_ascii=False) + "\n")
                writer.writerow({
                    key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
                    for key, value in record.items()
                })
        jsonl_temp.rename(jsonl_path)
        csv_temp.rename(csv_path)
    finally:
        jsonl_temp.unlink(missing_ok=True)
        csv_temp.unlink(missing_ok=True)
    return columns


def export_source_jsonl_csv(jsonl_path, csv_path):
    """Export one JSONL file or an ordered list of saved collection runs."""
    paths = jsonl_path if isinstance(jsonl_path, (list, tuple)) else [jsonl_path]

    def records():
        for path in paths:
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        record = json.loads(line)
                        if not isinstance(record, dict):
                            raise ValueError(f"Expected a record object in {path}")
                        yield record

    columns, seen, count = [], set(), 0
    for record in records():
        for key in record:
            if key not in seen:
                columns.append(key)
                seen.add(key)
        count += 1
    temporary = csv_path.with_suffix('.csv.tmp')
    if csv_path.exists():
        raise FileExistsError(csv_path)
    try:
        with temporary.open('x', encoding='utf-8', newline='') as output:
            writer = csv.DictWriter(output, fieldnames=columns, lineterminator='\n')
            if columns:
                writer.writeheader()
            for record in records():
                writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
                                 for key, value in record.items()})
        temporary.rename(csv_path)
    finally:
        temporary.unlink(missing_ok=True)
    return columns, count

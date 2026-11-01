# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Read candidate inventories and tester-generated profiles."""

import csv
import json
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STORAGE = Path(os.environ["OTA_STORAGE_DIR"]).resolve() if os.environ.get("OTA_STORAGE_DIR") else None
DATA = STORAGE / "data" if STORAGE else ROOT / "data"
GENERATED = STORAGE / "state" if STORAGE else ROOT / "ingestion_points/generated"
REFERENCES = Path(os.environ.get("OTA_REFERENCE_DIR", str(ROOT / "ingestion_points/raw"))).resolve()
SOURCES = GENERATED / "sources"


def latest_file(pattern):
    """Timestamped filenames sort by UTC run time; never mix profile runs."""
    files = sorted(GENERATED.glob(pattern))
    if not files:
        raise ValueError(f"No {pattern} found in {GENERATED}. Run the preceding step first.")
    return files[-1]


def load_candidates(path=None):
    path = Path(path) if path else latest_file("urls_*.csv")
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not {"source_id", "display_name", "source_url"}.issubset(reader.fieldnames or []):
            raise ValueError("Candidate CSV requires source_id, display_name and source_url.")
        sources = list(reader)
    ids = [row["source_id"] for row in sources]
    if any(not value.strip() for value in ids) or len(set(ids)) != len(ids):
        raise ValueError("Candidate source IDs must be nonempty and unique.")
    if any(not row["source_url"].strip() for row in sources):
        raise ValueError("Candidate URLs must be nonempty.")
    return sources, path


def load_testing_rules():
    path = ROOT / "ingestion_points" / "testing_rules.json"
    if not path.exists():
        return {}
    rules = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rules, dict) or any(not isinstance(value, dict) for value in rules.values()):
        raise ValueError("Testing rules must map source IDs to rule objects.")
    return rules


def source_folder(source_id):
    """Use source IDs as directory names without allowing path traversal."""
    if not isinstance(source_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", source_id):
        raise ValueError("Source IDs must use letters, digits, underscores, dots or hyphens.")
    folder = SOURCES / source_id
    if not folder.resolve().is_relative_to(SOURCES.resolve()):
        raise ValueError("Source directory must remain inside generated/sources/.")
    return folder


def load_sources():
    """Read the latest saved result per source, including failures and duplicates."""
    files = sorted(SOURCES.glob("*/profile.json"))
    if not files:
        raise ValueError("No source profiles found. Run python -m src.source_tester first.")
    sources = []
    seen = set()
    for path in files:
        source = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(source, dict):
            raise ValueError("Invalid source profile object.")
        for key in ("source_id", "display_name", "source_url"):
            if not isinstance(source.get(key), str) or not source[key].strip():
                raise ValueError(f"Source profile requires {key}.")
        if source["source_id"] in seen:
            raise ValueError("Generated profiles contain duplicate source IDs.")
        seen.add(source["source_id"])
        if source_folder(source["source_id"]).resolve() != path.resolve().parent:
            raise ValueError("Profile source ID does not match its directory.")
        if not isinstance(source.get("profile"), dict):
            raise ValueError("Generated source profile requires request settings.")
        sources.append(source)
    return sources

def load_profiles():
    return {source["source_id"]: source["profile"] for source in load_sources()}

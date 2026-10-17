---
status: accepted
---

# Generated BigQuery catalog lives under dbt/catalog/

Hand-maintaining or committing a multi‑10MB `models/sources.yml` made the dbt models tree unusable in editors and agents. Generated source YAML now lands at `dbt/catalog/sources.generated.yml` (gitignored), included via `model-paths`. Hand-written sources (`sources_ingestion.yml`, `sources_geoboundaries.yml`, etc.) stay under `models/`. Regenerate with `python data/local/scripts/generate_dbt_sources.py --refresh`.

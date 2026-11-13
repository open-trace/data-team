# Ingestion plan (transform-v2)

**UX:** `ingestion/notebooks/` to try loads → `scripts/` for reusable runs → `package/` for shared CLI/sources.

**Writes:** `landing_dev` (sandbox) and `raw_dev` (primordial source for dbt).  
**Transforms:** dbt from `raw_dev` through `mart_dev`. No Airbyte.

Project: `opentrace-prod-5ga4`. Location: `europe-west3`.

## Auth

Shared `ingest-dev` SA key outside the repo (`GOOGLE_APPLICATION_CREDENTIALS`). Admin: `data-eng/infra/ingestion/setup.sh`.

## CLI

```text
python -m ingestion list | check-auth | run <source> [--dry-run] [--limit] [--target]
python ingestion/scripts/run_source.py <source> …
```

Default target: `landing_dev`. Soft cutover: some tables may still be filled by transitional dbt runners until migrated.

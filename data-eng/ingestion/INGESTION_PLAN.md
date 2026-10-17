# OpenTrace repo-based ingestion — plan

Goal: ingestion jobs live in `data-eng/ingestion/` as normal Python, runnable from Cursor/VS Code. Under **transform-v2**, ingestion **writes `landing_dev` and `raw_dev`**. dbt treats **`raw_dev` as the primordial source** through `mart_dev`. No Airbyte for this path.

Project: `opentrace-prod-5ga4`. Location: `europe-west3`.

## Dataset split

| Dataset | Role |
|---------|------|
| `landing_dev` | Ingest sandbox (shared SA key; partitions expire) |
| `raw_dev` | Ingest-owned; primordial source for transformation |
| `staging_dev` → `intermediate_dev` → `mart_dev` | Transform-owned (dbt) |

Soft cutover: older dbt runners may still write some `landing` / `raw_dev` tables until migrated table-by-table into this package. Do not big-bang delete those models.

## Auth

Shared **`ingest-dev`** service-account JSON key **outside** the repo via `GOOGLE_APPLICATION_CREDENTIALS`. Admin: `data-eng/infra/ingestion/setup.sh`.

## CLI

```text
python -m ingestion list | check-auth | run <source> [--since] [--limit] [--dry-run] [--target]
```

Target: `--target` > `INGEST_TARGET_DATASET` > `landing_dev`.  
Guard: only allowed sandbox/prod targets when explicitly enabled (`INGEST_ALLOW_PROD=1` and `CI=true` for prod-like datasets).

## Load modes

Metadata columns + `replace_partition` / `merge` load jobs. Never `insert_rows_json`. Never create datasets.

## Templates

`hdx_wfp_prices` (CSV), `wb_pink_sheet` (XLSX), `wb_rtfp` (JSON page), `un_psdata` (JSON page).

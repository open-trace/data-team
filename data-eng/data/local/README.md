# Local development database (PostgreSQL)

A **local PostgreSQL database** holds a **partition** of BigQuery data used to curate and test ETL locally.

**Path note:** Renaming this tree to `data-eng/local/` is deferred (compose, Makefile, and scripts still use `data/local/`). Prefer `sync_all_tables.py --dataset raw_dev` over bronze-named aliases.

## Layers (schemas match BQ dataset ids)

| Schema | Role |
|--------|------|
| `landing` / `landing_dev` | Ingest-related (sync as needed) |
| `raw_dev` | Primordial transform source (ingest-owned in BQ) |
| `staging_dev` | Staging |
| `intermediate_dev` | Intermediate |
| `mart_dev` | Mart |

Ingest package: [`../../ingestion/`](../../ingestion/).

## Team collaboration

Each developer runs their own Postgres and sets `LOCAL_DB_URL` in `.env`. Schema DDL and sync scripts are shared; data is not committed.

## Layout

| Path | Purpose |
|------|---------|
| `schema/{landing,raw_dev,staging_dev,intermediate_dev,mart_dev}/` | Per-table DDL |
| `scripts/` | Sync / schema tools (`bq_schema_to_local_pg.py`, `sync_all_tables.py`) |
| `.env.example` | Template (do not commit `.env`) |

## Populate from BigQuery

1. Set `BQ_PROJECT`, `BQ_DATASET_LANDING=landing`, `BQ_DATASET_BRONZE=raw_dev`, `BQ_DATASET_SILVER=staging_dev`, `BQ_DATASET_INTERMEDIATE=intermediate_dev`, `BQ_DATASET_GOLD=mart_dev`, and `LOCAL_DB_URL`.
2. Generate DDL and create tables:

```bash
python data/local/scripts/bq_schema_to_local_pg.py
```

3. Sync data:

```bash
python data/local/scripts/sync_all_tables.py --dataset landing
python data/local/scripts/sync_all_tables.py --dataset raw_dev
python data/local/scripts/sync_all_tables.py --dataset staging_dev
python data/local/scripts/sync_all_tables.py --dataset intermediate_dev
python data/local/scripts/sync_all_tables.py --dataset mart_dev
```

Or one-shot: `bash data/local/scripts/populate_local_db.sh`

## Docker

```bash
cd data-eng
docker compose up -d postgres
docker compose --profile setup up --build setup
```

## Env

Copy `.env.example` → `.env`. For dbt/transforms use OAuth or a BQ-read key. For **ingest** into `landing_dev`, use the shared key in `data-eng/ingestion/.env` (outside the repo).

# dbt — OpenTrace data team

This dbt project runs against BigQuery. **Model folders and dbt targets match BigQuery dataset IDs** used in this project: `landing`, **`raw_dev`**, **`staging_dev`**, **`mart_dev`**. Env vars `BQ_DATASET_BRONZE`, `BQ_DATASET_SILVER`, and `BQ_DATASET_GOLD` still point at those physical datasets (names inherited from older “medallion” terminology).

Use **`--target raw_dev | staging_dev | mart_dev`** (or `*_sa` with a service account key).

## Prerequisites

- **BigQuery**: Datasets such as `landing`, `raw_dev`, `staging_dev`, `mart_dev` (your project may include additional datasets; see `BQ_DATASETS_INCLUDE` for cataloguing).
- **Credentials (local, recommended):** OAuth — `gcloud auth application-default login`. Set `BQ_PROJECT` and optional `BQ_DATASET_*` in `data/local/.env`.
- **Credentials (Docker/CI):** Service account key — set `GOOGLE_APPLICATION_CREDENTIALS` and targets `raw_dev_sa`, `staging_dev_sa`, `mart_dev_sa`.

## Environment variables

Set these in `data/local/.env` (or export in your shell).

| Variable | Required (local OAuth) | Description |
|----------|------------------------|-------------|
| `BQ_PROJECT` | Yes | GCP project ID. |
| `BQ_DATASET_LANDING` | No | Landing dataset (default `landing`). |
| `BQ_DATASET_BRONZE` | No | Raw layer dataset (default **`raw_dev`**). |
| `BQ_DATASET_SILVER` | No | Staging layer dataset (default **`staging_dev`**). |
| `BQ_DATASET_GOLD` | No | Mart dataset (default **`mart_dev`**). |
| `GOOGLE_APPLICATION_CREDENTIALS` | No (OAuth) / Yes (Docker) | Path to service account JSON for `*_sa` targets. |
| `DBT_TARGET` | No | Default target (default **`raw_dev`**). |

**OAuth example:**

```bash
BQ_PROJECT=opentrace-prod-5ga4
BQ_DATASET_BRONZE=raw_dev
BQ_DATASET_SILVER=staging_dev
BQ_DATASET_GOLD=mart_dev
DBT_TARGET=raw_dev
```

## Running dbt

From the `dbt` directory (with `DBT_PROFILES_DIR=.`):

```bash
dbt deps
dbt run --target raw_dev
dbt run --target staging_dev
dbt run --target mart_dev
```

Docker (service account):

```bash
docker compose --profile dbt run --rm dbt sh -c "dbt deps && dbt run --target raw_dev_sa"
```

## Pipeline levels

| Level     | dbt target      | Typical writes (env)   |
|-----------|-----------------|-------------------------|
| Landing   | (sources only)  | `BQ_DATASET_LANDING`    |
| Raw       | `raw_dev`       | `BQ_DATASET_BRONZE`     |
| Staging   | `staging_dev`   | `BQ_DATASET_SILVER`     |
| Mart      | `mart_dev`      | `BQ_DATASET_GOLD`       |

## Project layout

```
dbt/
├── dbt_project.yml
├── profiles.yml       # raw_dev, staging_dev, mart_dev (+ _sa)
├── models/
│   ├── sources.yml    # Generated from docs/bq_schema_catalog.json
│   ├── landing/
│   ├── raw_dev/
│   ├── staging_dev/
│   └── mart_dev/
```

In models, use **`{{ source('raw_dev', 'table') }}`**, **`source('staging_dev', ...)`**, **`source('landing', ...)`** — names must match those in `sources.yml` after `generate_dbt_sources.py`.

## Keeping sources in sync

```bash
python data/local/scripts/generate_dbt_sources.py --refresh
```

## Troubleshooting

- **Credentials**: `gcloud auth application-default login` and `BQ_PROJECT` for OAuth; for Docker use `*_sa` targets and a valid key path.
- **Wrong dataset**: Check `BQ_DATASET_*` and `--target`.

For repo-wide docs, see [data-eng README](../README.md).

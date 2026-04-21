# dbt — OpenTrace data team

This directory is a **dbt project** that runs against BigQuery with **dynamic dataset access** per pipeline level: **ingestion** (landing + bronze), **bronze** (landing, bronze, silver), **silver** (silver, gold), **gold** (silver, gold). One project, multiple **targets**; you choose the level with `--target bronze | silver | gold`.

## Prerequisites

- **BigQuery**: A GCP project with datasets for `landing`, `bronze`, `silver`, `gold` (create them in BQ if needed).
- **Credentials (local, recommended):** Use **OAuth** — run `gcloud auth application-default login` once. No key file; each dev uses their own Google identity. Set only `BQ_PROJECT` (and optional `BQ_DATASET_*`) in `data/local/.env`.
- **Credentials (Docker/CI):** Use a **service account key** — set `GOOGLE_APPLICATION_CREDENTIALS` in `.env` and use dbt targets `bronze_sa`, `silver_sa`, `gold_sa` (e.g. `dbt run --target bronze_sa`).

## Environment variables

Set these in `data/local/.env` (or export in your shell). The same file is used by local DB scripts and Docker.

| Variable | Required (local OAuth) | Description |
|----------|------------------------|-------------|
| `BQ_PROJECT` | Yes | GCP project ID (e.g. `opentrace-prod-5ga4`). |
| `BQ_DATASET_LANDING` | No | BigQuery dataset for landing (default `landing`). |
| `BQ_DATASET_BRONZE` | No | BigQuery dataset for bronze (default `bronze`). |
| `BQ_DATASET_SILVER` | No | BigQuery dataset for silver (default `silver`). |
| `BQ_DATASET_GOLD` | No | BigQuery dataset for gold (default `gold`). |
| `GOOGLE_APPLICATION_CREDENTIALS` | No (OAuth) / Yes (Docker) | Path to service account JSON key; only needed for Docker/CI or when using `*_sa` targets. |
| `DBT_TARGET` | No | Default dbt target when you don’t pass `--target` (default `bronze`). |

**Local (OAuth) example** — no key file:

```bash
BQ_PROJECT=opentrace-prod-5ga4
BQ_DATASET_BRONZE=bronze
BQ_DATASET_SILVER=silver
BQ_DATASET_GOLD=gold
# Run once: gcloud auth application-default login
```

**Docker / key-based** — add `GOOGLE_APPLICATION_CREDENTIALS=data/local/keys/your-key.json` and use `--target bronze_sa` (or `silver_sa`, `gold_sa`). Do **not** commit `.env` or the key file; they are gitignored.

## Running dbt

### Option 1: On your machine (recommended for development)

1. **Load env** (from repo root):

   ```bash
   cd /path/to/data-team
   set -a && source data/local/.env && set +a
   ```

   Or ensure the same vars are exported in your shell.

2. **Install dbt** (if not already):

   ```bash
   pip install dbt-bigquery
   ```

3. **Run from the `dbt` directory** and point dbt at this project’s `profiles.yml`:

   ```bash
   cd dbt
   export DBT_PROFILES_DIR=.
   dbt deps
   dbt run --target bronze
   dbt run --target silver
   dbt run --target gold
   ```
   (From repo root you can use `DBT_PROFILES_DIR=dbt dbt run --target bronze`.)

   Or a single target: `dbt run --target silver`.

4. **Tests and docs**:

   ```bash
   dbt test
   dbt docs generate
   dbt docs serve
   ```

### Option 2: Docker (same env and key as the rest of the repo)

Use the `dbt` service so anyone can pull the repo, add credentials to `data/local/.env` and place the key file, then run dbt without installing it locally.

1. **One-time**: Ensure `data/local/.env` and `data/local/keys/<your-key>.json` exist (and `GOOGLE_APPLICATION_CREDENTIALS` in `.env` points to that path).

2. **Build the image** (if not already built by `docker compose --profile setup up`):

   ```bash
   docker compose build
   ```

3. **Run dbt** with the `dbt` profile (mounts repo and loads `.env`):

   Use the **`_sa`** targets so the container uses the mounted key file:

   ```bash
   # Bronze (deps + run inside the container)
   docker compose --profile dbt run --rm dbt sh -c "dbt deps && dbt run --target bronze_sa"

   # Silver
   docker compose --profile dbt run --rm dbt dbt run --target silver_sa

   # Gold
   docker compose --profile dbt run --rm dbt dbt run --target gold_sa
   ```

   To override the default command and run tests or docs:

   ```bash
   docker compose --profile dbt run --rm dbt dbt test
   docker compose --profile dbt run --rm dbt dbt docs generate
   ```

The container uses `DBT_PROFILES_DIR=/app/dbt` and `GOOGLE_APPLICATION_CREDENTIALS=/app/data/local/keys/...` (mount repo so `data/local/keys/` is available). The compose file uses `env_file: data/local/.env`, so all `BQ_*` and `DBT_*` vars are set inside the container.

## Pipeline levels and dataset access

| Level     | dbt target | Reads from (datasets)     | Writes to (dataset) |
|----------|------------|---------------------------|----------------------|
| Ingestion| —          | landing, bronze           | landing / bronze (outside dbt) |
| Bronze   | `bronze`   | landing, bronze, silver   | bronze               |
| Silver   | `silver`   | silver, gold              | silver               |
| Gold     | `gold`     | silver, gold              | gold                 |

- **Ingestion** is your existing Python/Composer jobs; they use `BQ_PROJECT` and `BQ_DATASET_*` from `.env`; no dbt.
- **Bronze / Silver / Gold** are dbt: run `dbt run --target bronze`, `--target silver`, or `--target gold`. Dataset names are taken from env, so the same project works for every environment (dev/staging/prod) by changing `.env`.

## Project layout

```
dbt/
├── dbt_project.yml    # Project name, vars (dataset names from env), model config
├── profiles.yml       # BigQuery targets (bronze, silver, gold) using env vars
├── models/
│   ├── sources.yml   # Sources: landing, bronze, silver, gold (schema = var)
│   ├── bronze/       # Models that write to bronze
│   ├── silver/       # Models that write to silver
│   └── gold/         # Models that write to gold
├── analyses/
├── tests/
├── macros/
├── seeds/
├── snapshots/
└── packages.yml
```

- **sources.yml**: Defines `landing`, `bronze`, `silver`, `gold` with `database`/`schema` from `env_var('BQ_PROJECT')` and `var('*_dataset')`. Use `{{ source('bronze', 'table_name') }}` in models. **Generated** from the BQ schema catalog — see “Keeping sources in sync” below.
- **profiles.yml**: Three outputs `bronze`, `silver`, `gold`; each has its own `dataset` from env. Active target selects which dataset dbt writes to.
- Replace the example models (`stg_example`, `silver_example`, `gold_example`) with your real logic and add new models under the correct folder.

## Adding new models

- **Bronze**: Add SQL and schema under `models/bronze/`; read from `{{ source('landing', '...') }}` or `{{ source('bronze', '...') }}`; they will write to the bronze dataset when you run `--target bronze`.
- **Silver**: Add under `models/silver/`; use `{{ ref('...') }}` for bronze models and `{{ source('silver', '...') }}` if needed; write to silver with `--target silver`.
- **Gold**: Add under `models/gold/`; use `{{ ref('...') }}` and sources; write to gold with `--target gold`.

## Keeping sources in sync

`sources.yml` is **generated** from the BigQuery schema catalog so you don’t maintain table/column lists by hand. After BQ schema changes:

1. Refresh the catalog: `python data/local/scripts/bq_schema_catalog.py`
2. Regenerate dbt sources: `python data/local/scripts/generate_dbt_sources.py`

Or in one step: `python data/local/scripts/generate_dbt_sources.py --refresh`

The script reads `docs/bq_schema_catalog.json` and overwrites `dbt/models/sources.yml`. If the catalog is missing, it writes a minimal `sources.yml` (placeholder tables) so dbt still parses; run the two steps above to fill from BQ.

## Troubleshooting

- **“Set BQ_PROJECT” or “credentials not found”**: For local OAuth, run `gcloud auth application-default login` and set `BQ_PROJECT` in `.env`. For Docker or key-based auth, ensure `GOOGLE_APPLICATION_CREDENTIALS` points to a valid key file and use `--target bronze_sa` (or `silver_sa`/`gold_sa`). In Docker, the key must be under the repo (e.g. `data/local/keys/`) so the path inside the container exists.
- **Wrong dataset**: Check `BQ_DATASET_*` in `.env` and that you passed the correct `--target`.
- **Docker “key not found”**: Mount the repo (e.g. `.:/app`) and put the key at `data/local/keys/<file>.json`; set `GOOGLE_APPLICATION_CREDENTIALS=/app/data/local/keys/<file>.json` in `.env` or in the compose `environment` section.

For more on the repo’s data flow, see the main [README](../README.md) and [Architecture](../docs/ARCHITECTURE.md).

## GitHub + BigQuery CI setup

To make every dbt change visible in GitHub, this repository includes `.github/workflows/dbt-ci.yml`.
It runs on every push/PR that touches `dbt/**` and executes `dbt debug`, `dbt parse`, and `dbt build` against BigQuery.

Set these in your GitHub repository before enabling CI:

- **Repository secret**
  - `GCP_SERVICE_ACCOUNT_KEY`: full JSON content of your Google service account key.
- **Repository variables**
  - `BQ_PROJECT`
  - `BQ_DATASET_BRONZE` (optional, recommended)
  - `BQ_DATASET_SILVER` (required)
  - `BQ_DATASET_GOLD` (optional, recommended)
  - `DBT_BIGQUERY_LOCATION` (optional, default in profile is used if missing)

Suggested minimum BigQuery IAM roles for the CI service account:

- `roles/bigquery.jobUser`
- `roles/bigquery.dataViewer` on source datasets
- `roles/bigquery.dataEditor` on the target silver dataset

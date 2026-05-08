# Data Team — OpenTrace

Repository for the **data team**: bronze → silver → gold **databases**, **feature store**, and ML (training, evaluation, RAG, serving). Work is prototyped in IDEs and versioned here; production runs on **GCP** (BigQuery, Vertex AI, Composer).

The repo is split into two ownership areas:

- **`data-eng/`** — Ingestion config (Airbyte), **Airflow** DAGs, **dbt**, **Terraform**, warehouse SQL, notebooks for ETL design, and local/dev tooling (`data/local`). See **[data-eng/README.md](data-eng/README.md)**.
- **`ml/`** — Feature store code, training, evaluation, RAG, serving (`ml.*` Python package). See **[ml/README.md](ml/README.md)**.

At the repo root, **`data`**, **`dbt`**, **`infra`**, and **`data-pipelines`** are **symlinks** into `data-eng/` so existing paths (`data/local/…`, `cd dbt`) keep working.

## Architecture (high level)

```
Sources (APIs, GCS, BQ partitions)
        │
        ▼
   Ingestion (weather / market / satellite)
        │
        ▼
   BigQuery
   ├── Bronze database (raw, immutable)
   ├── Silver database (cleaned, normalized)
   └── Gold database (ML & analytics ready)
        │
        ▼
   Feature store (BQ + Python / Vertex AI)
        │
        ▼
   Vertex AI
   ├── Training pipelines
   ├── Model registry
   ├── RAG (vector DB + curated docs)
        │
        ▼
   Prediction + QA API
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for details.

## Repo layout

| Path | Purpose |
|------|---------|
| **`data-eng/`** | Data platform monolith: `dbt/`, `airflow/`, `airbyte/`, `infra/`, `data/`, `data-pipelines/`, `libs/`, `config/`. Start here for analytics engineering. |
| **`dbt/`** *(symlink)* | Same as `data-eng/dbt/` — BigQuery bronze → silver → gold. See [dbt/README.md](dbt/README.md). |
| **`data-pipelines/`** *(symlink)* | Same as `data-eng/data-pipelines/` — notebooks to design ETL. |
| **`data/`** *(symlink)* | Same as `data-eng/data/` — ingestion, `sql/`, validation, **`local/`** dev DB. |
| **`infra/`** *(symlink)* | Same as `data-eng/infra/` — Terraform modules and environments. |
| **`ml/`** | **ML / AI engineering**: `features/`, `training/`, `evaluation/`, `rag/`, `serving/` — prototype here; run on Vertex AI. |
| **`Makefile`** | Convenience targets (`make help`). |
| **`.github/`** | Issue templates, PR template, CI, SQL lint, ML tests, **data-eng** path workflows. |

## Getting started

**First-time clone?** Run the automated setup (builds Docker, starts Postgres, syncs dbt sources from BigQuery using your OAuth), then follow the step-by-step guide:

```bash
bash scripts/first_time_setup.sh
```

Full walkthrough (dbt usage, orchestration): **[docs/FIRST_TIME_SETUP.md](docs/FIRST_TIME_SETUP.md)**.

---

1. **Clone and install**
   ```bash
   git clone <repo-url> && cd data-team
   python -m venv .venv && source .venv/bin/activate  # or .venv\Scripts\activate on Windows
   pip install -r requirements.txt -r requirements-dev.txt
   ```

2. **Local partition DB (PostgreSQL recommended)**  
   - **Postgres only:** Run **`docker compose up -d`**. On first run, Postgres starts and **`datateam_local`** is created automatically.  
   - **Everything in Docker (Postgres + schema + bronze/silver/gold sync + GIS):** Ensure `data/local/.env` exists (copy from `.env.example`) and `data/local/keys/opentrace-bq-key.json` is in place, then run **`docker compose --profile setup up`**. The `setup` service runs `populate_local_db.sh` inside a container (creates tables from BigQuery schemas for bronze, silver, gold; syncs data for all three; loads the GIS CSV) and exits. Connect from your machine with **`LOCAL_DB_URL=postgresql://postgres:postgres@localhost:5432/datateam_local`**.  
   - **Host-only populate:** Alternatively run **`bash data/local/scripts/populate_local_db.sh`** on your machine after `docker compose up -d`. Use one database per developer (see [data/local/README.md](data/local/README.md)).

3. **Develop pipelines**  
   Use notebooks in `data-pipelines/` (ingestion, bronze, silver, gold), optionally against the local DB. When ready, add or update SQL in `data/sql/bronze_to_silver/` and `data/sql/silver_to_gold/` for BigQuery.

4. **dbt (BigQuery bronze → silver → gold)**  
   One dbt project under `dbt/` with **dynamic dataset access** per level. Anyone can pull the repo, add credentials to `data/local/.env` (and a key file), and run dbt locally or in Docker.  
   - **Env:** Set `BQ_PROJECT`, `BQ_DATASET_LANDING`, `BQ_DATASET_BRONZE`, `BQ_DATASET_SILVER`, `BQ_DATASET_GOLD`, and `GOOGLE_APPLICATION_CREDENTIALS` in `data/local/.env` (see `data/local/.env.example`).  
   - **Local:** From repo root, load `.env` then run `cd dbt && dbt deps && dbt run --target bronze` (or `silver` / `gold`).  
   - **Docker:** Build once (`docker compose build` or run `docker compose --profile setup up`), then run dbt with  
     `docker compose --profile dbt run --rm dbt sh -c "dbt deps && dbt run --target bronze"`  
     (replace `bronze` with `silver` or `gold` as needed).  
   Full guide: **[dbt/README.md](dbt/README.md)**.

5. **Develop ML**  
   Use `ml/features/`, `ml/training/`, etc. Tests under `ml/` run in GitHub Actions (`ml-tests.yml`).  
   **RAG in Docker:** after populating `data/local/vector_db` and secrets in `data/local/.env`, run `docker compose --profile rag up --build rag-api` (and optionally `rag-streamlit`). See **[ml/rag/README.md](ml/rag/README.md)**.

6. **CI/CD**  
   Push to `main` or `develop` (or open a PR): CI runs lint/format and notebook checks; SQL lint runs on `data/sql` and `data-eng/dbt/models`; ML tests run on `ml/`. Terraform fmt, dbt YAML checks, and DAG syntax checks run when files under `data-eng/infra`, `data-eng/dbt`, `data-eng/airflow`, or `data-eng/airbyte` change.

## Conventions

- **No secrets in repo** — use env vars or GCP Secret Manager; see `.gitignore`.
- **SQL** — BigQuery dialect; keep production-ready SQL in `data/sql/` (operates on bronze/silver/gold databases and feature store).
- **Notebooks** — For exploration and design; export logic to SQL or Python modules for production.

## Links

- [**First-time setup**](docs/FIRST_TIME_SETUP.md) — Script + step-by-step guide for new clones (Docker, dbt sources, pipelines, orchestration)
- [Architecture](docs/ARCHITECTURE.md)
- [BigQuery schema catalog](docs/BIGQUERY_SCHEMA.md) (run `python data/local/scripts/bq_schema_catalog.py` to refresh)
- [Data engineering layout](data-eng/README.md)
- [dbt (BigQuery bronze/silver/gold)](dbt/README.md)
- [Data pipelines](data-pipelines/README.md)
- [Data (ingestion, SQL, validation, local DB)](data/README.md)
- [Local partition DB](data/local/README.md)
- [ML](ml/README.md)
- [Infra](infra/README.md)

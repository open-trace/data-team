# Data Team — OpenTrace

Repository for the **data team**: bronze → silver → gold **databases**, **feature store**, and ML (training, evaluation, RAG, serving). Work is prototyped in IDEs and versioned here; production runs on **GCP** (BigQuery, Vertex AI, Composer).

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
| **`data-pipelines/`** | Notebooks and scripts to design ETL: `ingestion/`, `bronze/`, `silver/`, `gold/`. Logic is implemented in BigQuery using SQL under `data/sql/`. |
| **`data/`** | `ingestion/` (weather, market, satellite), `sql/` (bronze→silver, silver→gold databases), `validation/`, and **`local/`** (partition DB for curating ETL). |
| **`ml/`** | **Feature store** logic (`features/`), `training/`, `evaluation/`, `rag/`, `serving/` — prototype here; run on Vertex AI. |
| **`infra/`** | `terraform/` (GCP resources), `composer/` (Airflow DAGs for scheduled pipelines). |
| **`.github/`** | Issue templates, PR template, workflows: CI, SQL lint, ML tests. |

## Getting started

1. **Clone and install**
   ```bash
   git clone <repo-url> && cd data-team
   python -m venv .venv && source .venv/bin/activate  # or .venv\Scripts\activate on Windows
   pip install -r requirements.txt -r requirements-dev.txt
   ```

2. **Local partition DB (PostgreSQL recommended)**  
   Start Postgres: `docker compose up -d`, then create your DB (e.g. `CREATE DATABASE datateam_local;`). Copy `data/local/.env.example` to `data/local/.env` and set `LOCAL_DB_URL=postgresql://postgres:postgres@localhost:5432/datateam_local` and your BigQuery settings. Run `python data/local/scripts/bq_partition_to_local.py` to sync a partition. Use one database per developer for smooth collaboration (see [data/local/README.md](data/local/README.md)).

3. **Develop pipelines**  
   Use notebooks in `data-pipelines/` (ingestion, bronze, silver, gold), optionally against the local DB. When ready, add or update SQL in `data/sql/bronze_to_silver/` and `data/sql/silver_to_gold/` for BigQuery.

4. **Develop ML**  
   Use `ml/features/`, `ml/training/`, etc. Tests under `ml/` run in GitHub Actions (`ml-tests.yml`).

5. **CI/CD**  
   Push to `main` or `develop` (or open a PR): CI runs lint/format and notebook checks; SQL lint runs on `data/sql`; ML tests run on `ml/`.

## Conventions

- **No secrets in repo** — use env vars or GCP Secret Manager; see `.gitignore`.
- **SQL** — BigQuery dialect; keep production-ready SQL in `data/sql/` (operates on bronze/silver/gold databases and feature store).
- **Notebooks** — For exploration and design; export logic to SQL or Python modules for production.

## Links

- [Architecture](docs/ARCHITECTURE.md)
- [Data pipelines](data-pipelines/README.md)
- [Data (ingestion, SQL, validation, local DB)](data/README.md)
- [Local partition DB](data/local/README.md)
- [ML](ml/README.md)
- [Infra](infra/README.md)

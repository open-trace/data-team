# Architecture

This doc describes how the data team’s work fits together: from ingestion and the **bronze, silver, and gold databases** in BigQuery to the **feature store** and Vertex AI (training, RAG, serving).

## Principles

- **Prototype in the repo** — Notebooks and SQL in this repo are the source of truth for logic.
- **Run in GCP** — BigQuery runs the SQL pipelines; Vertex AI runs training, RAG, and serving.
- **Version and review** — All changes go through GitHub (branches, PRs, CI).

## Roles and local partition DB

- **Data engineers** work on GCP: they ingest datasets and store data in the **bronze database** (and beyond) in **BigQuery**.
- **Analysts** run **EDA** on that data and produce **reports**; the team uses these to decide how to continue with the pipeline.
- The team then takes a **partition** of the data and stores it in a **local database** (see `data/local/`). That local DB is used to **curate the ETL pipeline** in the IDE: develop and test bronze → silver → gold logic against real-but-small data, then promote the same logic to BigQuery.

So: full data stays in the bronze/silver/gold databases in BigQuery; a chosen partition is synced locally for fast, offline pipeline development and validation.

## Data flow

### 1. Ingestion

- **Where:** `data/ingestion/` (weather, market, satellite) and `data-pipelines/ingestion/`.
- **What:** Pull data from APIs, GCS, or BigQuery partitions into the pipeline.
- **Output:** Data lands in the **bronze database** (raw, e.g. BQ dataset or GCS → BQ load).

### 2. Bronze → Silver → Gold databases (BigQuery)

We use **databases** (BQ datasets), not ad-hoc tables:

- **Bronze database:** Raw, immutable. Partitioned by date/source as needed.
- **Silver database:** Cleaned, normalized, deduplicated. Business-level entities.
- **Gold database:** Aggregated, ML-ready, analytics-ready.

**Where the logic lives:**

- Design and prototyping: `data-pipelines/bronze/`, `silver/`, `gold/` (notebooks). These (and the SQL in `data/sql/`) can be developed and tested against the **local partition DB** (`data/local/`) before running on full BigQuery data.
- Production SQL: `data/sql/bronze_to_silver/`, `data/sql/silver_to_gold/` (read/write between these databases).
- Orchestration: Composer DAGs in `infra/composer/` (scheduled in GCP).

### 3. Feature store

- **Where:** `ml/features/` (definitions and pipelines); production feature store lives in GCP (e.g. Vertex AI Feature Store or a dedicated BQ dataset).
- **What:** Features are built from the **gold** (and silver) **databases**; logic is prototyped here and deployed to the feature store.
- **Consumers:** Training pipelines and serving.

### 4. Vertex AI

- **Training:** Pipelines and model code in `ml/training/`; run on Vertex AI; register models in the model registry.
- **Evaluation:** `ml/evaluation/` — metrics, comparisons, validation datasets.
- **RAG:** `ml/rag/` — chunking, embeddings, vector store (e.g. Vertex AI Vector Search), curated docs.
- **Serving:** `ml/serving/` — inference code and config for Vertex AI endpoints; feeds the **Prediction + QA API**.

## GitHub ↔ GCP

| Concern | GitHub (this repo) | GCP |
|--------|--------------------|-----|
| Code & SQL | Source of truth, PRs, CI | — |
| Lint / tests | `.github/workflows/` (ci, sql-lint, ml-tests) | — |
| Schedule & run | — | Composer, Cloud Scheduler |
| Storage & compute | — | BigQuery, GCS, Vertex AI |
| Secrets | Never committed | Secret Manager, IAM |

## CI/CD (GitHub Actions)

- **CI (`ci.yml`):** Lint (e.g. Ruff), format (Black), basic notebook checks on push/PR to `main`/`develop`.
- **SQL lint (`sql-lint.yml`):** Lint SQL in `data/sql` (and pipeline SQL) when those paths change.
- **ML tests (`ml-tests.yml`):** Run tests under `ml/` when ML code or requirements change.

Orchestration and deployment to GCP (e.g. Terraform apply, Composer DAG sync) can be added as separate workflows or run from a trusted environment.

## Summary

- **Thinking and prototyping:** IDEs + this repo (notebooks, SQL, Python).
- **Versioning and review:** GitHub (branches, PRs, issue/PR templates).
- **Automated checks:** GitHub Actions (CI, SQL lint, ML tests).
- **Execution:** BigQuery (bronze/silver/gold databases, feature store), Vertex AI (training, RAG, serving), Composer (scheduling).

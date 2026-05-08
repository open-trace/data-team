# Data engineering (`data-eng/`)

This tree owns **ingestion → orchestration → transformations → warehouse** for OpenTrace: Airbyte-style ingestion config, Apache Airflow (Composer) DAGs, dbt on BigQuery, Terraform for GCP, and local/dev tooling (`data/local`, Postgres sync scripts).

**ML and AI** code lives at repo root in **`ml/`** (Python package `ml.*`). That boundary keeps orchestration and analytics engineering separate from model training, RAG, and serving.

## Layout

| Path | Role |
|------|------|
| **`infra/`** | Terraform — BigQuery datasets, Composer, Airbyte deployment, IAM (foundation). |
| **`airflow/`** | DAGs only: trigger ingestion, run dbt, tests. Python helpers under `tasks/`. |
| **`dbt/`** | Transformations in BigQuery (this project uses **bronze → silver → gold** naming; aligns with raw → staging → marts conceptually). |
| **`airbyte/`** | Version-controlled connector/connection definitions and helper scripts (not UI-only). |
| **`data/`** | Ingestion scripts, hand-written SQL (`sql/`), validation, **local dev DB** under `data/local/`. |
| **`data-pipelines/`** | Notebook-first ETL design (non-prod exploration). |
| **`libs/python/`** | Shared Python libraries for pipelines (BigQuery helpers, logging, etc.). |
| **`notebooks/`** | Optional scratch exploration (`exploration/`, `prototyping/`, `archive/`). |
| **`docker/`** | Notes and overrides for local compose images (root `docker-compose.yml` stays canonical). |
| **`config/`** | Environment templates — prefer secrets via Secret Manager in prod. |

## End-to-end flow

```text
Sources → Airbyte (ingestion) → BigQuery (raw / landing)
        → Airflow triggers dbt → bronze → silver → gold
        → Analytics / BI / downstream ML features
```

## Governance

1. **Airbyte / ingestion config** — load only (into raw or landing datasets).  
2. **dbt** — transforms only.  
3. **Airflow** — orchestration only (no business SQL in DAG files beyond glue).  
4. **Terraform** — infrastructure only.  
5. **Notebooks** — never production paths.

## Repo root symlinks

From the repository root, **`data`**, **`dbt`**, and **`infra`** are symlinks into `data-eng/` so existing docs and scripts that use `data/local/…`, `cd dbt`, etc. keep working without churn.

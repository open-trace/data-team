# Infrastructure (Terraform)

Foundation for the data platform: BigQuery, Composer, IAM, storage, and ingest SA setup.

**Active ingest path:** [`ingestion/setup.sh`](./ingestion/setup.sh) + Python [`../ingestion/`](../ingestion/). Airbyte Terraform modules are **retired** for new work (transform-v2).

## Layout

| Path | Purpose |
|------|---------|
| **`ingestion/`** | Shared ingest-dev SA / `landing_dev` IAM setup script |
| **`modules/bigquery/`** | Datasets: landing*, raw/staging/mart |
| **`modules/phase2_airbyte_composer/`** | Retired — do not expand |
| **`modules/airbyte/`** | Retired — do not expand |
| **`modules/`** | Other stubs: `airflow_composer/`, `iam/`, `storage/` |
| **`environments/`** | Root modules per environment |
| **`global/`** | Org-wide or shared resources |
| **`composer/`** | Non-DAG Composer notes — DAGs in `data-eng/airflow/dags/` |

Do not commit `.tfstate` or secrets; use remote state and Secret Manager.

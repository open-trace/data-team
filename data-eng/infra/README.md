# Infrastructure (Terraform)

Foundation for the data platform: BigQuery, Composer, Airbyte-related resources, IAM, and storage.

## Layout

| Path | Purpose |
|------|---------|
| **`modules/bigquery/`** | Datasets: `landing`, raw/staging/mart dev + prod. |
| **`modules/phase2_airbyte_composer/`** | Phase 2: `sa-airbyte-bq-writer`, landing IAM, Secret Manager, firewall (Composer → Airbyte VM). |
| **`modules/`** | Other stubs: `airflow_composer/`, `iam/`, `storage/`, legacy `airbyte/`. |
| **`environments/`** | Root modules per environment (`dev/`, `staging/`, `prod/`) with `main.tf`, `variables.tf`, `backend.tf`. |
| **`global/`** | Org-wide or shared resources. |
| **`terraform/`** | Legacy stub; new work uses `modules/` + `environments/`. |
| **`composer/`** | Non-DAG Composer notes only — **DAGs** live in `data-eng/airflow/dags/`. |

Do not commit `.tfstate` or secrets; use remote state (e.g. GCS backend) and Secret Manager.

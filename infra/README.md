# Infra

GCP infrastructure as code and orchestration. Used to run the data pipelines and ML workloads defined in this repo.

## Layout

- **`terraform/`** — Project, BigQuery datasets, GCS buckets, service accounts, Vertex AI resources, etc.
- **`composer/`** — Airflow/Composer DAGs and dependencies that schedule:
  - Ingestion and bronze/silver/gold SQL in BigQuery
  - Optional triggers for Vertex AI training or batch prediction

## Usage

- Develop and review Terraform in PRs; apply via CI or manual workflow from a trusted environment.
- DAGs in `composer/` are synced to the Composer environment (e.g. via Cloud Build or manual upload).
- Do not commit `.tfstate` or secrets; use remote state and secret managers.

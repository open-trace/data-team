# CI workflow definitions (mirrored)

GitHub Actions only loads workflows from **`.github/workflows/`** at the repository root.

Path-triggered jobs for data engineering live there:

| Workflow file | Triggers on changes under |
|---------------|---------------------------|
| `data-eng-terraform.yml` | `data-eng/infra/**` |
| `data-eng-dbt.yml` | `data-eng/dbt/**` |
| `data-eng-airflow.yml` | `data-eng/airflow/**` |
| `data-eng-ingest.yml` | `data-eng/ingestion/**` |

Airbyte workflows are retired with the Airbyte tree (`data-eng/archive/airbyte/`). Prefer `data-eng-ingest.yml`.

This folder documents that mapping; edit the YAML under `.github/workflows/` when changing pipelines.

Ingestion admin IAM/key setup (not CI): [`data-eng/infra/ingestion/setup.sh`](../../infra/ingestion/setup.sh).

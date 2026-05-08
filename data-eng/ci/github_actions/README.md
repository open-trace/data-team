# CI workflow definitions (mirrored)

GitHub Actions only loads workflows from **`.github/workflows/`** at the repository root.

Path-triggered jobs for data engineering live there:

| Workflow file | Triggers on changes under |
|---------------|---------------------------|
| `data-eng-terraform.yml` | `data-eng/infra/**` |
| `data-eng-dbt.yml` | `data-eng/dbt/**` |
| `data-eng-airflow.yml` | `data-eng/airflow/**` |
| `data-eng-airbyte.yml` | `data-eng/airbyte/**` |

This folder documents that mapping; edit the YAML under `.github/workflows/` when changing pipelines.

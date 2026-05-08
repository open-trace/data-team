# BigQuery destination (Airbyte → `landing`)

Configure in the Airbyte UI **Destination → BigQuery** (or equivalent connector id).

| Setting | Value |
|--------|--------|
| Project ID | `opentrace-prod-5ga4` (or your `BQ_PROJECT`) |
| Dataset / default dataset | **`landing`** |
| Dataset location | **`europe-west3`** (must match project policy) |
| Credentials | Service account JSON for **`sa-airbyte-bq-writer`** (or your naming) |

## IAM (least privilege)

The destination service account should have:

- **On dataset `landing`**: `roles/bigquery.dataEditor` (dataset-level IAM binding)
- **On the GCP project**: `roles/bigquery.jobUser`

Do **not** grant this SA write access to `raw_*`, `staging_*`, or `mart_*` — ingestion targets **`landing`** only; dbt promotes downstream.

## Repo env alignment

Same logical dataset as `BQ_DATASET_LANDING=landing` in `data/local/.env`.

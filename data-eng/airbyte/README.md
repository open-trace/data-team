# Airbyte configuration

Ingestion runs in **Airbyte** → BigQuery dataset **`landing`** only. Downstream **dbt** reads `landing` via `sources.yml` and builds `raw_*` → `staging_*` → `mart_*` (dev vs prod targets).

## Layout

- **`connections/`** — [registry](connections/README.md) (connection IDs, owner, cadence metadata).
- **`destinations/`** — [BigQuery destination](destinations/BIGQUERY.md) settings aligned with Terraform IAM.
- **`scripts/`** — API helpers (e.g. trigger sync by connection id).

## Local + phase 2

- **Local**: Airbyte via [official Docker deployment](https://docs.airbyte.com/deploying-airbyte/local-deployment); Airflow via [`docker-compose.local.yml`](../docker-compose.local.yml). See [docker/README.md](../docker/README.md).
- **Phase 2 (GCE + Composer)**: same registry and destination; move Airbyte URL to private IP and store API token in Secret Manager (see `infra/modules/phase2_airbyte_composer/`).

Apply connection changes through the Airbyte UI or Automation API; keep this folder as the source of truth for **IDs and ownership**.


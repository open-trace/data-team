# Configuration templates

- **`constants.yaml`** — non-secret defaults (region names, dataset prefixes).
- **Per-environment files** — copy patterns from `../data/local/.env.example` for local dev; in GCP use Secret Manager for credentials.

Authoritative local developer env for BigQuery + Postgres sync remains **`data/local/.env`** (reachable via the root `data` symlink).

## Local ingestion stack

See **[`docker/README.md`](../docker/README.md)** (Airflow Compose) and **`docs/LOCAL_STACK.md`** (Airbyte + dbt verification).


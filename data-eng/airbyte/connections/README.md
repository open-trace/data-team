# Airbyte connection registry

Fill **`registry.yaml`** from **`registry.example.yaml`** after you create connections in Airbyte (copy connection UUID from the UI).

- **`connection_id`**: Required for Airflow `airbyte_sync` DAG and `trigger_sync.py`.
- **`destination_dataset`**: Should always be **`landing`** for this project (single shared ingestion dataset).
- **`owner`**: Who coordinates syncs for shared `landing` (avoid conflicting experiments).

Do **not** commit secrets. If `registry.yaml` contains nothing sensitive, you may commit it; otherwise add it to `.gitignore` and use CI variables.

See also [destinations/BIGQUERY.md](destinations/BIGQUERY.md).

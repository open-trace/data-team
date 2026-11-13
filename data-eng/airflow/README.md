# Airflow (Composer)

Orchestration only: schedule ingest jobs and `dbt run` / `dbt test`. **No Airbyte** on the active path (transform-v2).

Legacy Airbyte sync DAG: [`../archive/airflow-airbyte/`](../archive/airflow-airbyte/).

## Layout

- `dags/ingestion/` — triggers for Python ingest (replace retired Airbyte DAG)
- `dags/transformations/` — dbt pipeline DAGs
- `dags/monitoring/` — data quality / freshness checks
- `tasks/` — Python callables, not SQL transforms
- `plugins/` — Composer plugins when needed

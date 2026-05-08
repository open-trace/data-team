# Airflow (Composer)

Orchestration only: trigger Airbyte syncs, wait for completion, run `dbt run` / `dbt test`, and lightweight monitoring DAGs.

Historical note: `infra/composer/` held DAG placeholders; active DAGs should live here under `dags/` and deploy to Cloud Composer from this folder.

## Layout

- `dags/ingestion/` — Airbyte-related DAGs  
- `dags/transformations/` — dbt pipeline DAGs  
- `dags/monitoring/` — data quality / freshness checks  
- `tasks/` — Python callables (API clients, sensors), not SQL transforms  
- `plugins/` — Composer plugins when needed  

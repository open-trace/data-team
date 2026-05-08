# Local Docker stack (Airbyte + Airflow + BigQuery env)

## Overview

- **Airbyte**: run with the [official local deployment](https://docs.airbyte.com/deploying-airbyte/local-deployment) (their Compose stack). Default API: `http://localhost:8000`.
- **Airflow**: [`docker-compose.local.yml`](../docker-compose.local.yml) runs **`airflow standalone`** (one container, SQLite metadata) and mounts [`../airflow/dags`](../airflow/dags).

## Start Airflow

From the **repository root**:

```bash
docker compose -f data-eng/docker-compose.local.yml up -d
```

- **UI**: `http://localhost:8080`
- **Login**: `airflow` / `airflow` (set via `_AIRFLOW_WWW_USER_*` in the compose file)

## Start Airbyte

Follow Airbyte’s current docs (example):

```bash
git clone --depth 1 https://github.com/airbytehq/airbyte.git
cd airbyte && ./run-ab-platform.sh   # command name may change — use their README
```

Configure a **BigQuery** destination: project `BQ_PROJECT`, dataset **`landing`**, location **`europe-west3`**, service account JSON with write access to `landing` only.

## Point Airflow at Airbyte

The Compose file sets `AIRBYTE_URL=http://host.docker.internal:8000` so the **scheduler** can call the Airbyte API when Airbyte runs on your **host** (Docker Desktop Mac/Windows).

- **Linux**: override when starting:  
  `AIRBYTE_URL=http://172.17.0.1:8000 docker compose -f data-eng/docker-compose.local.yml up -d`  
  (or your host IP; if Airbyte runs in another Compose stack, join networks or use the container IP.)

Optional: `AIRBYTE_CLIENT_TOKEN` if you enable Bearer auth on Airbyte.

## Related

- [docs/LOCAL_STACK.md](../docs/LOCAL_STACK.md) — ingestion → `sources.yml` → dbt
- [airbyte/README.md](../airbyte/README.md) — connection registry and API scripts

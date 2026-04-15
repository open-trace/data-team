# Local stack: Airbyte + Airflow + BigQuery + dbt

End-to-end flow for **phase 1** (local Docker, single shared `landing`).

## 1. Prerequisites

- Docker
- GCP access: `gcloud auth application-default login`
- `data-eng/data/local/.env` with `BQ_PROJECT` and layer dataset env vars (see `.env.example`)

## 2. Start Airbyte (official install)

Install per [Airbyte local deployment](https://docs.airbyte.com/deploying-airbyte/local-deployment). UI typically `http://localhost:8000`.

Create a **BigQuery destination** → dataset **`landing`**, location **`europe-west3`**, using a service account that can write **only** to `landing`.

## 3. Start local Airflow

From repo root:

```bash
docker compose -f data-eng/docker-compose.local.yml up -d
```

Open `http://localhost:8080` (user `airflow` / `airflow`).

Set Airflow **Variables** (Admin → Variables):

| Key | Example value |
|-----|----------------|
| `AIRBYTE_CONNECTION_ID` | `a1b2c3d4-...` (from Airbyte connection settings) |

Or set container env `AIRBYTE_SYNC_CONNECTIONS` / `AIRBYTE_CONNECTION_ID` in `docker-compose.local.yml`.

Set `AIRBYTE_URL` if Airbyte is not reachable at `http://host.docker.internal:8000` (Linux hosts often need a different host IP).

## 4. Register connections in the repo

Copy `data-eng/airbyte/connections/registry.example.yaml` → `registry.yaml` and record IDs for the team (see [connections/README.md](../airbyte/connections/README.md)).

## 5. Run ingestion sync from Airflow

Unpause `airbyte_sync` DAG and trigger, or wait for schedule. Tasks: **trigger** → **wait** → **trigger_dbt_pipeline** (expects `dbt_pipeline` DAG to exist).

CLI alternative:

```bash
python data-eng/airbyte/scripts/trigger_sync.py <connection_uuid>
```

## 6. Refresh dbt sources and validate

```bash
set -a && source data-eng/data/local/.env && set +a
python data-eng/data/local/scripts/generate_dbt_sources.py --refresh
cd data-eng/dbt && DBT_PROFILES_DIR=. dbt parse
dbt run --target raw_dev --select ...
```

## Shared `landing` (multi-developer)

Coordinate connection ownership; avoid concurrent schema experiments on the same tables. See [airbyte/README.md](../airbyte/README.md).

## Phase 2 (later)

Move Airbyte to GCE (private IP), Composer triggers `AIRBYTE_URL` on VPC; apply Terraform [`phase2_airbyte_composer`](../infra/modules/phase2_airbyte_composer/README.md) + BigQuery module.

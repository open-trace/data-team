# Local Docker notes

Canonical compose files live at `data-eng/docker-compose.yml` and `data-eng/docker-compose.local.yml`.

## Airflow (optional local)

```bash
docker compose -f data-eng/docker-compose.local.yml up -d
```

- UI: `http://localhost:8080` (see compose for credentials)

## Ingestion and dbt

- **Ingest:** Python package [`../ingestion/`](../ingestion/) → `landing_dev` / `raw_dev` (no Airbyte).
- **dbt:** [`../dbt/`](../dbt/) — transform-v2; regenerate sources with `python data/local/scripts/generate_dbt_sources.py --refresh`.

Retired Airbyte local stack notes: [`../docs/archive/LOCAL_STACK.md`](../docs/archive/LOCAL_STACK.md).

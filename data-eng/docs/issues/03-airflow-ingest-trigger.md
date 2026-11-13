## Parent

PRD: data-eng transform-v2 reorganization

## What to build

Replace the archived Airbyte sync DAG with an Airflow/Composer DAG (or documented schedule) that triggers Python ingest and then dbt, so orchestration matches the active load path.

## Acceptance criteria

- [ ] No active DAG depends on Airbyte API
- [ ] Documented how Composer invokes ingest + dbt for at least one source
- [ ] Archived Airbyte DAG remains under `archive/` only

## Blocked by

- 02-ingest-writes-raw-dev-pilot (preferred, not hard)

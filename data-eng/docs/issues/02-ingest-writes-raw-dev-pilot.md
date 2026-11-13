## Parent

PRD: data-eng transform-v2 reorganization

## What to build

Pilot one existing ingest source so the Python ingest package writes (or copies) into `raw_dev` after `landing_dev`, matching the transform-v2 contract, without changing dbt staging/intermediate/mart SQL.

## Acceptance criteria

- [ ] One source documents and demonstrates write path into `raw_dev`
- [ ] IAM/setup notes cover `raw_dev` write for ingest-dev SA
- [ ] dbt model SQL for staging→mart unchanged
- [ ] Soft cutover: do not delete the corresponding dbt raw_dev runner in this slice

## Blocked by

None - can start immediately

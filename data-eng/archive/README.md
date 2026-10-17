# Archive (not active)

Paths here are **not** part of the day-to-day engineer workflow.

| Path | Was | Why archived |
| ---- | --- | ------------ |
| `airbyte/` | Airbyte connection registry + scripts | Python `ingestion/` is the only load path (transform-v2 / ADR 0001) |
| `data-sql/` | `data/sql/` bronze_to_silver / silver_to_gold | Production transforms live in `dbt/` |

Do not restore these into the active tree without a new ADR.
